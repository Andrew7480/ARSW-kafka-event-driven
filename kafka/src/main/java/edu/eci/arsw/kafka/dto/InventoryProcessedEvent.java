package edu.eci.arsw.kafka.dto;

import java.time.Instant;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class InventoryProcessedEvent {
    private String inventoryId;
    private String orderId;
    private String customerId;
    private String status;
    private Instant occurredAt;
}
