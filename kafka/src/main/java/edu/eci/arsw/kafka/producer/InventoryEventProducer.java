package edu.eci.arsw.kafka.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import edu.eci.arsw.kafka.dto.InventoryProcessedEvent;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class InventoryEventProducer {
    private final KafkaTemplate<String, InventoryProcessedEvent> kafkaTemplate;

    public void publish(InventoryProcessedEvent event) {
        kafkaTemplate.send("inventory", event.getOrderId(), event);
    }
}
