package io.paramix.notify.worker;

import java.util.ArrayList;
import java.util.List;

import io.paramix.notify.config.KafkaErrorHandlers;
import io.paramix.notify.config.NotifyProperties;
import io.paramix.notify.domain.Channel;
import io.paramix.notify.domain.DeliveryMessage;
import io.paramix.notify.domain.Priority;
import io.paramix.notify.domain.Topics;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

/**
 * Teslimat topic'leri için kanal × öncelik başına ayrı consumer grubu: {@code worker-sms-critical}, ...
 * Böylece e-posta sağlayıcısı yavaşlasa SMS beklemez; kampanya OTP'nin thread'lerini işgal etmez.
 * {@code notify.worker.channels} ile pod'lar kanala göre ayrılabilir (SMS pod'ları, e-posta pod'ları...).
 */
@Component
@ConditionalOnProperty(name = "notify.roles.worker", havingValue = "true", matchIfMissing = true)
public class WorkerContainers implements SmartLifecycle {

    private final List<ConcurrentMessageListenerContainer<Object, Object>> containers = new ArrayList<>();
    private volatile boolean running;

    public WorkerContainers(ConcurrentKafkaListenerContainerFactory<Object, Object> factory,
                            DeliveryProcessor processor, ObjectMapper json, NotifyProperties props) {
        for (Channel channel : props.worker().channels()) {
            for (Priority priority : Priority.values()) {
                String topic = Topics.delivery(channel, priority);
                ConcurrentMessageListenerContainer<Object, Object> c = factory.createContainer(topic);
                c.getContainerProperties().setGroupId("worker-" + topic.replace('.', '-'));
                c.setConcurrency(props.worker().concurrency().get(priority));
                c.setBeanName("worker-" + topic);
                c.setupMessageListener((MessageListener<Object, Object>) rec -> processor.process(read(json, rec)));
                c.setCommonErrorHandler(KafkaErrorHandlers.create());
                containers.add(c);
            }
        }
    }

    private static DeliveryMessage read(ObjectMapper json, ConsumerRecord<Object, Object> rec) {
        try {
            return json.readValue((String) rec.value(), DeliveryMessage.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Geçersiz teslimat mesajı", e);
        }
    }

    /** Tüm consumer'lar partition aldı mı? (testler ve readiness için) */
    public boolean allAssigned() {
        return running && containers.stream().allMatch(c -> c.getAssignedPartitions() != null
                && !c.getAssignedPartitions().isEmpty());
    }

    @Override
    public void start() {
        containers.forEach(ConcurrentMessageListenerContainer::start);
        running = true;
    }

    @Override
    public void stop() {
        containers.forEach(ConcurrentMessageListenerContainer::stop);
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
