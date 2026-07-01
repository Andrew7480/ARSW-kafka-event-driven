package edu.eci.arsw.kafka.consumer;

import java.time.Instant;
import java.util.UUID;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import edu.eci.arsw.kafka.dto.InventoryProcessedEvent;
import edu.eci.arsw.kafka.dto.OrderCreatedEvent;
import edu.eci.arsw.kafka.producer.InventoryEventProducer;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class InventoryServiceConsumer {
    private final InventoryEventProducer inventoryProducer;

    @KafkaListener(topics = "orders", groupId = "inventory-service")
    public void consume(OrderCreatedEvent event) {
        System.out.println("Evento recibido en inventory-service: " + event.getOrderId());

        boolean reserved = event.getTotal() <= 300000;
        InventoryProcessedEvent inventoryEvent = new InventoryProcessedEvent(
                "INV-" + UUID.randomUUID(),
                event.getOrderId(),
                event.getCustomerId(),
                reserved ? "RESERVED" : "REJECTED",
                Instant.now());
        inventoryProducer.publish(inventoryEvent);
    }
}
