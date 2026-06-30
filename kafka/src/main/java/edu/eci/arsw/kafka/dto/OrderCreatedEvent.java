package edu.eci.arsw.kafka.dto;

import java.time.Instant;

import lombok.Data;

@Data
public class OrderCreatedEvent {
    private String orderId;
    private String customerId;
    private Double total;
    private String status;
    private Instant occurredAt;
}
