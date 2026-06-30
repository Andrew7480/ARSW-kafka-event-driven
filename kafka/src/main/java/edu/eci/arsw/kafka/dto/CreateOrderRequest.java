package edu.eci.arsw.kafka.dto;

import lombok.Data;

@Data
public class CreateOrderRequest {
    private String customerId;
    private Double total;
}
