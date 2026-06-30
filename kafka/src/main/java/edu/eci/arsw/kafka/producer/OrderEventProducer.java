package edu.eci.arsw.kafka.producer;

import edu.eci.arsw.kafka.dto.OrderCreatedEvent;
import lombok.Data;

@Data
public class OrderEventProducer {
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public void publishOrderCreated(OrderCreatedEvent event) {
        kafkaTemplate.send("orders", event.getOrderId(), event);
    }
}
