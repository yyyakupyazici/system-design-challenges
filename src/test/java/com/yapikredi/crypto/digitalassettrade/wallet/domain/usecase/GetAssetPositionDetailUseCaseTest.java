package com.yapikredi.crypto.digitalassettrade.wallet.domain.usecase;

import com.yapikredi.crypto.digitalassettrade.wallet.asset.common.domain.model.Currency;
import com.yapikredi.crypto.digitalassettrade.wallet.asset.common.domain.port.CurrencyPort;
import com.yapikredi.crypto.digitalassettrade.wallet.domain.model.CustomerAccountStatement;
import com.yapikredi.crypto.digitalassettrade.wallet.domain.model.CustomerAssetSnapshot;
import com.yapikredi.crypto.digitalassettrade.wallet.domain.model.QuoteCurrency;
import com.yapikredi.crypto.digitalassettrade.wallet.domain.model.VirtualAccount;
import com.yapikredi.crypto.digitalassettrade.wallet.domain.model.VirtualAccountPosition;
import com.yapikredi.crypto.digitalassettrade.wallet.domain.port.CustomerAccountStatementPort;
import com.yapikredi.crypto.digitalassettrade.wallet.domain.port.CustomerAssetSnapshotPort;
import com.yapikredi.crypto.digitalassettrade.wallet.domain.port.VirtualAccountPort;
import com.yapikredi.crypto.digitalassettrade.wallet.domain.port.VirtualAccountPositionPort;
import com.yapikredi.crypto.digitalassettrade.wallet.domain.usecase.GetAssetPositionDetailUseCase.DailyDiff;
import com.yapikredi.crypto.digitalassettrade.wallet.domain.usecase.GetAssetPositionDetailUseCase.Input;
import com.yapikredi.crypto.digitalassettrade.wallet.domain.usecase.GetAssetPositionDetailUseCase.Output;
import com.yapikredi.crypto.digitalassettrade.wallet.marketdata.domain.constant.PriceType;
import com.yapikredi.crypto.digitalassettrade.wallet.marketdata.domain.model.CurrencyPairPrice;
import com.yapikredi.crypto.digitalassettrade.wallet.marketdata.domain.port.MarketDataPort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.quality.Strictness;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
class GetAssetPositionDetailUseCaseTest {

    private static final Integer CUSTOMER_ID = 42;
    private static final Long CUSTOMER_ID_AS_LONG = 42L;
    private static final Integer ASSET_ID = 7;
    private static final String SYMBOL = "BTC";
    private static final int PRECISION = 8;

    @Mock
    private VirtualAccountPort virtualAccountPort;
    @Mock
    private VirtualAccountPositionPort virtualAccountPositionPort;
    @Mock
    private MarketDataPort marketDataPort;
    @Mock
    private CurrencyPort currencyPort;
    @Mock
    private CustomerAssetSnapshotPort customerAssetSnapshotPort;
    @Mock
    private CustomerAccountStatementPort customerAccountStatementPort;

    @InjectMocks
    private GetAssetPositionDetailUseCase useCase;

    private Currency currency;

    @BeforeEach
    void setUp() {
        currency = mock(Currency.class, withSettings().strictness(Strictness.LENIENT));
        when(currency.getSymbol()).thenReturn(SYMBOL);
        when(currency.getPrecision()).thenReturn(PRECISION);
        lenient().when(currencyPort.getById(ASSET_ID)).thenReturn(currency);
    }

    @Nested
    class Handle {

        @Test
        void returnsEmptyOutputWhenVirtualAccountDoesNotExist() {
            when(virtualAccountPort.getVirtualAccountByCustomerIdAndCurrencySymbol(CUSTOMER_ID, SYMBOL))
                    .thenReturn(null);

            Output output = useCase.handle(input(QuoteCurrency.USD));

            assertEmptyOutput(output);
            verifyNoInteractions(virtualAccountPositionPort, marketDataPort,
                    customerAssetSnapshotPort, customerAccountStatementPort);
        }

        @Test
        void returnsEmptyOutputWhenPositionDoesNotExist() {
            VirtualAccount account = givenVirtualAccount();
            var accountId = account.id();
            when(virtualAccountPositionPort.findByVirtualAccountIdAndCurrencySymbol(accountId, SYMBOL))
                    .thenReturn(Optional.empty());

            Output output = useCase.handle(input(QuoteCurrency.USD));

            assertEmptyOutput(output);
            verifyNoInteractions(marketDataPort, customerAssetSnapshotPort, customerAccountStatementPort);
        }

        @ParameterizedTest
        @MethodSource("com.yapikredi.crypto.digitalassettrade.wallet.domain.usecase.GetAssetPositionDetailUseCaseTest#zeroPositions")
        void returnsZeroTotalDiffButKeepsDailyDiffWhenAmountOrAverageCostIsZero(
                BigDecimal amount,
                BigDecimal averageCost
        ) {
            givenPosition(amount, averageCost, QuoteCurrency.USD);
            givenMarketPrice("150");
            givenSnapshots(snapshot(SYMBOL, "2", "140", QuoteCurrency.USD));
            givenTransactions(Map.of());

            Output output = useCase.handle(input(QuoteCurrency.USD));

            assertThat(output.getAverageCost()).isEqualTo(averageCost);
            assertThat(output.getLastPrice()).isEqualByComparingTo("150");
            assertThat(output.getTotalDiffAmount()).isEqualByComparingTo(BigDecimal.ZERO);
            assertThat(output.getTotalDiffPercentage()).isEqualTo(0.0);
            assertThat(output.getDailyDiffAmount()).isEqualByComparingTo("20.00");
            assertThat(output.getDailyDiffPercentage()).isEqualTo(7.14);
        }

        @ParameterizedTest
        @EnumSource(QuoteCurrency.class)
        void calculatesTotalAndDailyProfitForEachQuoteCurrency(QuoteCurrency quoteCurrency) {
            givenPosition(new BigDecimal("2"), new BigDecimal("100"), quoteCurrency);
            givenMarketPrice("150");
            givenSnapshots(snapshot(SYMBOL, "2", "140", quoteCurrency));
            givenTransactions(Map.of());

            Output output = useCase.handle(input(quoteCurrency));

            assertThat(output.getAverageCost()).isEqualByComparingTo("100");
            assertThat(output.getLastPrice()).isEqualByComparingTo("150");
            assertThat(output.getTotalDiffAmount()).isEqualByComparingTo("100");
            assertThat(output.getTotalDiffPercentage()).isEqualTo(50.0);
            assertThat(output.getDailyDiffAmount()).isEqualByComparingTo("20.00");
            assertThat(output.getDailyDiffPercentage()).isEqualTo(7.14);
        }

        @Test
        void calculatesLossAndRoundsPercentageHalfUp() {
            givenPosition(new BigDecimal("1"), new BigDecimal("300"), QuoteCurrency.USD);
            givenMarketPrice("200");
            givenSnapshots();
            givenTransactions(Map.of());

            Output output = useCase.handle(input(QuoteCurrency.USD));

            assertThat(output.getTotalDiffAmount()).isEqualByComparingTo("-100");
            assertThat(output.getTotalDiffPercentage()).isEqualTo(-33.33);
            assertThat(output.getDailyDiffAmount()).isEqualByComparingTo(BigDecimal.ZERO);
            assertThat(output.getDailyDiffPercentage()).isEqualTo(0.0);
        }

        @ParameterizedTest
        @EnumSource(QuoteCurrency.class)
        void requestsMarketPriceForAssetAndQuoteCurrency(QuoteCurrency quoteCurrency) {
            givenPosition(new BigDecimal("1"), new BigDecimal("100"), quoteCurrency);
            givenMarketPrice("100");
            givenSnapshots();
            givenTransactions(Map.of());

            useCase.handle(input(quoteCurrency));

            ArgumentCaptor<CurrencyPairPrice> captor = ArgumentCaptor.forClass(CurrencyPairPrice.class);
            verify(marketDataPort).getMarketDataPrice(captor.capture());
            CurrencyPairPrice request = captor.getValue();
            assertThat(request.getPriceType()).isEqualTo(PriceType.MARKET_PRICE);
            assertThat(request.getPair().getBaseCurrencySymbol()).isEqualTo(SYMBOL);
            assertThat(request.getPair().getQuoteCurrencySymbol()).isEqualTo(quoteCurrency.name());
        }
    }

    @Nested
    class ResolveDailyDiff {

        @Test
        void returnsEmptyWhenSnapshotPortIsMissing() {
            GetAssetPositionDetailUseCase withoutSnapshotPort = new GetAssetPositionDetailUseCase(
                    virtualAccountPort, virtualAccountPositionPort, marketDataPort, currencyPort,
                    null, customerAccountStatementPort);

            DailyDiff dailyDiff = withoutSnapshotPort.resolveDailyDiff(
                    input(QuoteCurrency.USD), currency, BigDecimal.TEN);

            assertEmptyDailyDiff(dailyDiff);
            verifyNoInteractions(customerAccountStatementPort);
        }

        @Test
        void returnsEmptyWhenStatementPortIsMissing() {
            GetAssetPositionDetailUseCase withoutStatementPort = new GetAssetPositionDetailUseCase(
                    virtualAccountPort, virtualAccountPositionPort, marketDataPort, currencyPort,
                    customerAssetSnapshotPort, null);

            DailyDiff dailyDiff = withoutStatementPort.resolveDailyDiff(
                    input(QuoteCurrency.USD), currency, BigDecimal.TEN);

            assertEmptyDailyDiff(dailyDiff);
            verifyNoInteractions(customerAssetSnapshotPort);
        }

        @Test
        void returnsEmptyWhenPortsReturnNull() {
            when(customerAssetSnapshotPort.findByCustomerIdAndSnapshotDate(eq(CUSTOMER_ID_AS_LONG), any()))
                    .thenReturn(null);
            givenTransactions(null);

            DailyDiff dailyDiff = useCase.resolveDailyDiff(input(QuoteCurrency.USD), currency, BigDecimal.TEN);

            assertEmptyDailyDiff(dailyDiff);
        }

        @Test
        void ignoresSnapshotsAndTransactionsOfOtherAssets() {
            CustomerAssetSnapshot withoutCryptoAsset = lenientDeepMock(CustomerAssetSnapshot.class);
            when(withoutCryptoAsset.getCryptoAsset()).thenReturn(null);
            CustomerAssetSnapshot withoutCurrency = lenientDeepMock(CustomerAssetSnapshot.class);
            when(withoutCurrency.getCryptoAsset().getCurrency()).thenReturn(null);
            CustomerAssetSnapshot otherAsset = snapshot("ETH", "5", "10", QuoteCurrency.USD);

            givenSnapshots(withoutCryptoAsset, withoutCurrency, otherAsset);
            givenTransactions(Map.of("ETH", List.of(statement("BUY", "1", "10", "0", QuoteCurrency.USD))));

            DailyDiff dailyDiff = useCase.resolveDailyDiff(input(QuoteCurrency.USD), currency, BigDecimal.TEN);

            assertEmptyDailyDiff(dailyDiff);
        }

        @ParameterizedTest
        @EnumSource(QuoteCurrency.class)
        void calculatesDailyDiffForAllTransactionTypesWithoutSnapshot(QuoteCurrency quoteCurrency) {
            List<CustomerAccountStatement> transactions = Arrays.asList(
                    statement("buy", "1", "90", "1", quoteCurrency),           // +1    flow  +91    ref +91
                    statement("SELL", "-0.5", "110", "0.5", quoteCurrency),   // -0.5  flow  -54.5
                    statement("DEPOSIT", "2", "95", "0", quoteCurrency),       // +2    flow  +190   ref +190
                    statement("INCOMING", "1", "80", "0", quoteCurrency),      // +1    flow  +80    ref +80
                    statement("WITHDRAW", "1", "100", "2", quoteCurrency),     // -1    flow  -98
                    statement("OUTGOING", "0.5", "100", "0", quoteCurrency),   // -0.5  flow  -50
                    statement("REWARD", "0.1", "100", "0", quoteCurrency),     // +0.1
                    statement("AIRDROP", "0.2", "100", "0", quoteCurrency),    // +0.2
                    statement("STAKE", "5", "100", "0", quoteCurrency),        // ignored
                    statement(null, "3", "100", "0", quoteCurrency),           // ignored
                    statement("BUY", null, "100", null, quoteCurrency),        // no amount
                    null
            );
            givenSnapshots();
            givenTransactions(Map.of(SYMBOL, transactions));

            DailyDiff dailyDiff = useCase.resolveDailyDiff(input(quoteCurrency), currency, new BigDecimal("100"));

            // final position 2.3 * 100 = 230, cumulative flow 158.5 -> pnl 71.5; reference 361
            assertThat(dailyDiff.amount()).isEqualTo(new BigDecimal("71.50"));
            assertThat(dailyDiff.percentage()).isEqualTo(19.80);
        }

        @Test
        void calculatesDailyDiffWithSnapshotAndTransactions() {
            givenSnapshots(snapshot(SYMBOL, "1", "100", QuoteCurrency.EUR));
            givenTransactions(Map.of(SYMBOL, List.of(statement("BUY", "1", "110", "1", QuoteCurrency.EUR))));

            DailyDiff dailyDiff = useCase.resolveDailyDiff(input(QuoteCurrency.EUR), currency, new BigDecimal("120"));

            // 2 * 120 - 100 - 111 = 29; reference 100 + 111 = 211
            assertThat(dailyDiff.amount()).isEqualTo(new BigDecimal("29.00"));
            assertThat(dailyDiff.percentage()).isEqualTo(13.74);
        }

        @Test
        void treatsMissingSnapshotValuesAsZero() {
            CustomerAssetSnapshot snapshot = snapshot(SYMBOL, null, null, QuoteCurrency.TL);
            givenSnapshots(snapshot);
            givenTransactions(Map.of(SYMBOL, List.of(statement("DEPOSIT", "1", "100", null, QuoteCurrency.TL))));

            DailyDiff dailyDiff = useCase.resolveDailyDiff(input(QuoteCurrency.TL), currency, new BigDecimal("100"));

            assertThat(dailyDiff.amount()).isEqualByComparingTo(BigDecimal.ZERO);
            assertThat(dailyDiff.percentage()).isEqualTo(0.0);
        }

        @Test
        void returnsZeroPercentageWhenReferenceIsZero() {
            givenSnapshots();
            givenTransactions(Map.of(SYMBOL, List.of(statement("REWARD", "1", "0", "0", QuoteCurrency.USD))));

            DailyDiff dailyDiff = useCase.resolveDailyDiff(input(QuoteCurrency.USD), currency, new BigDecimal("100"));

            assertThat(dailyDiff.amount()).isEqualTo(new BigDecimal("100.00"));
            assertThat(dailyDiff.percentage()).isEqualTo(0.0);
        }

        @Test
        void roundsDailyDiffDown() {
            givenSnapshots(snapshot(SYMBOL, "3", "1", QuoteCurrency.USD));
            givenTransactions(Map.of());

            DailyDiff dailyDiff = useCase.resolveDailyDiff(
                    input(QuoteCurrency.USD), currency, new BigDecimal("1.00999"));

            // pnl 0.02997, reference 3 -> 0.999%
            assertThat(dailyDiff.amount()).isEqualTo(new BigDecimal("0.02"));
            assertThat(dailyDiff.percentage()).isEqualTo(0.99);
        }
    }

    @Nested
    class CalculateDailyDiffFromSnapshot {

        @Test
        void returnsEmptyWhenThereIsNoSnapshotAndNoTransaction() {
            DailyDiff dailyDiff = useCase.calculateDailyDiffFromSnapshot(
                    currency, null, List.of(), BigDecimal.TEN, QuoteCurrency.USD);

            assertEmptyDailyDiff(dailyDiff);
        }

        @Test
        void calculatesDailyDiffFromSnapshotAndTransactions() {
            CustomerAssetSnapshot snapshot = snapshot(SYMBOL, "1", "100", QuoteCurrency.USD);
            List<CustomerAccountStatement> transactions =
                    List.of(statement("BUY", "1", "110", "1", QuoteCurrency.USD));

            DailyDiff dailyDiff = useCase.calculateDailyDiffFromSnapshot(
                    currency, snapshot, transactions, new BigDecimal("120"), QuoteCurrency.USD);

            assertThat(dailyDiff.amount()).isEqualTo(new BigDecimal("29.00"));
            assertThat(dailyDiff.percentage()).isEqualTo(13.74);
        }

        @Test
        void calculatesDailyDiffFromTransactionsOnly() {
            List<CustomerAccountStatement> transactions =
                    List.of(statement("DEPOSIT", "2", "50", "0", QuoteCurrency.TL));

            DailyDiff dailyDiff = useCase.calculateDailyDiffFromSnapshot(
                    currency, null, transactions, new BigDecimal("45"), QuoteCurrency.TL);

            // 2 * 45 - 100 = -10; reference 100
            assertThat(dailyDiff.amount()).isEqualTo(new BigDecimal("-10.00"));
            assertThat(dailyDiff.percentage()).isEqualTo(-10.0);
        }
    }

    static Stream<Arguments> zeroPositions() {
        return Stream.of(
                Arguments.of(null, new BigDecimal("100")),
                Arguments.of(new BigDecimal("2"), null),
                Arguments.of(BigDecimal.ZERO, new BigDecimal("100")),
                Arguments.of(new BigDecimal("2"), BigDecimal.ZERO)
        );
    }

    private Input input(QuoteCurrency quoteCurrency) {
        return Input.builder()
                .customerId(CUSTOMER_ID)
                .assetId(ASSET_ID)
                .quoteCurrency(quoteCurrency)
                .build();
    }

    private VirtualAccount givenVirtualAccount() {
        VirtualAccount account = mock(VirtualAccount.class);
        when(virtualAccountPort.getVirtualAccountByCustomerIdAndCurrencySymbol(CUSTOMER_ID, SYMBOL))
                .thenReturn(account);
        return account;
    }

    private void givenPosition(BigDecimal totalAmount, BigDecimal averageCost, QuoteCurrency quoteCurrency) {
        VirtualAccountPosition position = mock(VirtualAccountPosition.class, withSettings().strictness(Strictness.LENIENT));
        when(position.getTotalAmount()).thenReturn(totalAmount);
        switch (quoteCurrency) {
            case USD -> when(position.getAverageCostUsd()).thenReturn(averageCost);
            case EUR -> when(position.getAverageCostEur()).thenReturn(averageCost);
            case TL -> when(position.getAverageCostTry()).thenReturn(averageCost);
        }

        VirtualAccount account = givenVirtualAccount();
        var accountId = account.id();
        when(virtualAccountPositionPort.findByVirtualAccountIdAndCurrencySymbol(accountId, SYMBOL))
                .thenReturn(Optional.of(position));
    }

    private void givenMarketPrice(String price) {
        when(marketDataPort.getMarketDataPrice(any())).thenReturn(new BigDecimal(price));
    }

    private void givenSnapshots(CustomerAssetSnapshot... snapshots) {
        when(customerAssetSnapshotPort.findByCustomerIdAndSnapshotDate(eq(CUSTOMER_ID_AS_LONG), any()))
                .thenReturn(List.of(snapshots));
    }

    private void givenTransactions(Map<String, List<CustomerAccountStatement>> transactions) {
        when(customerAccountStatementPort.getTodayCustomerTransactions(CUSTOMER_ID_AS_LONG))
                .thenReturn(transactions);
    }

    private static CustomerAssetSnapshot snapshot(
            String symbol,
            String amount,
            String parity,
            QuoteCurrency quoteCurrency
    ) {
        CustomerAssetSnapshot snapshot = lenientDeepMock(CustomerAssetSnapshot.class);
        lenient().when(snapshot.getCryptoAsset().getCurrency().getSymbol()).thenReturn(symbol);
        when(snapshot.getAmount()).thenReturn(decimal(amount));
        switch (quoteCurrency) {
            case USD -> when(snapshot.getUsdParity()).thenReturn(decimal(parity));
            case EUR -> when(snapshot.getEurParity()).thenReturn(decimal(parity));
            case TL -> when(snapshot.getTryParity()).thenReturn(decimal(parity));
        }
        return snapshot;
    }

    private static CustomerAccountStatement statement(
            String direction,
            String amount,
            String parity,
            String commission,
            QuoteCurrency quoteCurrency
    ) {
        CustomerAccountStatement statement =
                mock(CustomerAccountStatement.class, withSettings().strictness(Strictness.LENIENT));
        when(statement.getDirection()).thenReturn(direction);
        when(statement.getAmount()).thenReturn(decimal(amount));
        switch (quoteCurrency) {
            case USD -> {
                when(statement.getUsdParity()).thenReturn(decimal(parity));
                when(statement.getCommissionUsdEquivalentAmount()).thenReturn(decimal(commission));
            }
            case EUR -> {
                when(statement.getEurParity()).thenReturn(decimal(parity));
                when(statement.getCommissionEurEquivalentAmount()).thenReturn(decimal(commission));
            }
            case TL -> {
                when(statement.getTryParity()).thenReturn(decimal(parity));
                when(statement.getCommissionTryEquivalentAmount()).thenReturn(decimal(commission));
            }
        }
        return statement;
    }

    private static <T> T lenientDeepMock(Class<T> type) {
        return mock(type, withSettings().defaultAnswer(RETURNS_DEEP_STUBS).strictness(Strictness.LENIENT));
    }

    private static BigDecimal decimal(String value) {
        return value == null ? null : new BigDecimal(value);
    }

    private static void assertEmptyOutput(Output output) {
        assertThat(output.getAverageCost()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(output.getTotalDiffAmount()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(output.getLastPrice()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(output.getTotalDiffPercentage()).isEqualTo(0.0);
        assertThat(output.getDailyDiffAmount()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(output.getDailyDiffPercentage()).isEqualTo(0.0);
    }

    private static void assertEmptyDailyDiff(DailyDiff dailyDiff) {
        assertThat(dailyDiff.amount()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(dailyDiff.percentage()).isEqualTo(0.0);
    }
}
