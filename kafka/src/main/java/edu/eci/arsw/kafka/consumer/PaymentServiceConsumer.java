package edu.eci.arsw.kafka.consumer;

import java.time.Instant;
import java.util.UUID;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import edu.eci.arsw.kafka.dto.OrderCreatedEvent;
import edu.eci.arsw.kafka.dto.PaymentProcessedEvent;
import edu.eci.arsw.kafka.producer.PaymentEventProducer;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class PaymentServiceConsumer {
    private final PaymentEventProducer paymentProducer;

    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void consume(OrderCreatedEvent event) {
        System.out.println("Evento recibido en payment-service: " + event.getOrderId());

        boolean approved = event.getTotal() <= 250000;
        PaymentProcessedEvent paymentEvent = new PaymentProcessedEvent(
                "PAY-" + UUID.randomUUID(),
                event.getOrderId(),
                event.getCustomerId(),
                event.getTotal(),
                approved ? "APPROVED" : "REJECTED",
                Instant.now());
        paymentProducer.publish(paymentEvent);
    }
}
