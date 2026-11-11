package io.paramix.payreq;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class PaymentRequestApplication {

    public static void main(String[] args) {
        run(args);
    }

    /** Ayar dosyası "payment-request.yml": aynı JVM'de başka bir Spring uygulamasıyla çalışınca karışmasın. */
    public static ConfigurableApplicationContext run(String... args) {
        return new SpringApplicationBuilder(PaymentRequestApplication.class)
                .properties("spring.config.name=payment-request")
                .run(args);
    }
}
