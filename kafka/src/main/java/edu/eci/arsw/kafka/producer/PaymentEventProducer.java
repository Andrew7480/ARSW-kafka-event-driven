package edu.eci.arsw.kafka.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import edu.eci.arsw.kafka.dto.PaymentProcessedEvent;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class PaymentEventProducer {
    private final KafkaTemplate<String, PaymentProcessedEvent> kafkaTemplate;

    public void publish(PaymentProcessedEvent event) {
        kafkaTemplate.send("payments", event.getOrderId(), event);
    }
}
