package edu.eci.arsw.kafka.controller;

import java.time.Instant;
import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import edu.eci.arsw.kafka.dto.CreateOrderRequest;
import edu.eci.arsw.kafka.dto.OrderCreatedEvent;
import edu.eci.arsw.kafka.producer.OrderEventProducer;
import lombok.Data;

@RestController
@RequestMapping("/orders")
@Data
public class OrderController {
    private final OrderEventProducer producer;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public OrderCreatedEvent createOrder(@RequestBody CreateOrderRequest request) {
        OrderCreatedEvent event = new OrderCreatedEvent(
                "ORD-" + UUID.randomUUID(),
                request.getCustomerId(),
                request.getTotal(),
                "CREATED",
                Instant.now());
        producer.publishOrderCreated(event);
        return event;
    }

}
