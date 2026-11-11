#!/usr/bin/env bash
# Provizyon servisi demo turu. Önce: docker compose up --build  (iki instance: 8081 ve 8082)
# Gereken: curl, jq, python3 (UUIDv7 üretmek için)
set -euo pipefail

A=${A:-http://localhost:8081}
B=${B:-http://localhost:8082}
CUSTOMER=${CUSTOMER:-$((RANDOM + 100000))}

uuid7() {
  python3 -c 'import os,time; ms=int(time.time()*1000); r=bytearray(os.urandom(10))
b=ms.to_bytes(6,"big")+bytes([0x70|(r[0]&0x0F),r[1],0x80|(r[2]&0x3F)])+bytes(r[3:10])
h=b.hex(); print(f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}")'
}
post() { # url key body
  curl -s -D /tmp/demo-headers -o /tmp/demo-body -w '%{http_code}' -X POST "$1" \
    -H 'Content-Type: application/json' -H "X-Customer-Id: $CUSTOMER" ${2:+-H "Idempotency-Key: $2"} -d "$3"
}
show() { echo "  → HTTP $1 replayed=$(grep -i '^idempotent-replayed' /tmp/demo-headers | tr -d '\r' | awk '{print $2}') $(jq -c "$2" /tmp/demo-body)"; }
title() { printf '\n\033[1m%s\033[0m\n' "$*"; }

title "0) Müşteri $CUSTOMER'a 1000 TRY yatır (banka referansı ile; aynı bildirim iki kez gelir)"
for i in 1 2; do
  code=$(curl -s -o /tmp/demo-body -w '%{http_code}' -X POST "$A/internal/deposits" -H 'Content-Type: application/json' \
    -d "{\"customerId\":$CUSTOMER,\"asset\":\"TRY\",\"amount\":\"1000\",\"source\":\"BANK\",\"externalRef\":\"eft-$CUSTOMER\"}")
  echo "  → HTTP $code (201 = yazıldı, 200 = zaten vardı)"
done

title "1) Aynı Idempotency-Key ile 20 paralel istek, İKİ FARKLI POD'a (10 + 10)"
KEY=$(uuid7); REF="order-$CUSTOMER-1"
BODY="{\"asset\":\"TRY\",\"amount\":\"250\",\"operationType\":\"TRADE\",\"operationRef\":\"$REF\"}"
for i in $(seq 1 10); do for URL in $A $B; do
  curl -s -o /dev/null -w '%{http_code}\n' -X POST "$URL/v1/provisions" -H 'Content-Type: application/json' \
    -H "X-Customer-Id: $CUSTOMER" -H "Idempotency-Key: $KEY" -d "$BODY" &
done; done | sort | uniq -c | sed 's/^/  /'
wait
curl -s "$A/v1/balances" -H "X-Customer-Id: $CUSTOMER" | jq -c '.[] | {asset, available, held}' | sed 's/^/  bakiye: /'

title "2) Aynı key, farklı tutar → 422 (sessizce eski cevabı dönmek tehlikeli olurdu)"
show "$(post "$A/v1/provisions" "$KEY" "${BODY/250/900}")" '{code, detail}'

title "3) Aynı emir (operationRef), YENİ key → 409 (iş seviyesinde idempotency)"
show "$(post "$B/v1/provisions" "$(uuid7)" "$BODY")" '{code, provisionId: .context.provisionId}'

title "4) Kısmi capture: 250 bloke, emir 240'a doldu"
post "$A/v1/provisions" "$KEY" "$BODY" >/dev/null; PID=$(jq -r .id /tmp/demo-body)   # replay → aynı provizyon
CKEY=$(uuid7)
show "$(post "$A/v1/provisions/$PID/capture" "$CKEY" '{"amount":"240"}')" '{status, capturedAmount, releasedAmount}'
echo "  aynı capture tekrar (aynı key):"
show "$(post "$B/v1/provisions/$PID/capture" "$CKEY" '{"amount":"240"}')" '{status, capturedAmount}'

title "5) Yetersiz bakiye de bir sonuçtur: kaydedilir ve tekrar oynatılır"
K5=$(uuid7); B5='{"asset":"TRY","amount":"5000","operationType":"TRADE","operationRef":"order-big"}'
show "$(post "$A/v1/provisions" "$K5" "$B5")" '{code}'
show "$(post "$B/v1/provisions" "$K5" "$B5")" '{code}'

title "6) Kripto çekim: 202 → worker imzalar, yayınlar, onay bekler → capture"
curl -s -o /dev/null -X POST "$A/internal/deposits" -H 'Content-Type: application/json' \
  -d "{\"customerId\":$CUSTOMER,\"asset\":\"USDT\",\"amount\":\"500\",\"source\":\"CHAIN\",\"externalRef\":\"tron:tx-$CUSTOMER:0\"}"
show "$(post "$A/v1/withdrawals" "$(uuid7)" '{"asset":"USDT","amount":"120.5","destination":"TXyzDemo"}')" '{id, status}'
WID=$(jq -r .id /tmp/demo-body)
for i in $(seq 1 15); do
  S=$(curl -s "$A/v1/withdrawals/$WID" -H "X-Customer-Id: $CUSTOMER" | jq -r .status)
  echo "  durum: $S"; [ "$S" = CONFIRMED ] && break; sleep 1
done
curl -s "$A/v1/balances" -H "X-Customer-Id: $CUSTOMER" | jq -c '.[] | {asset, available, held}' | sed 's/^/  bakiye: /'

title "7) Mutabakat (boş liste = defter tutarlı)"
curl -s "$A/internal/reconciliation" | jq -c . | sed 's/^/  /'
