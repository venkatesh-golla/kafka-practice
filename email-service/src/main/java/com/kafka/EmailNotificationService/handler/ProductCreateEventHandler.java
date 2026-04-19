package com.kafka.EmailNotificationService.handler;

import com.kafka.core.ProductCreatedDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics = "product-created-events-topic", groupId = "product-created-events-v2")
public class ProductCreateEventHandler {
  private final Logger LOGGER = LoggerFactory.getLogger(ProductCreateEventHandler.class);

  @KafkaHandler
  public void handle(ProductCreatedDTO productCreatedDTO) {
    LOGGER.info(
        "Received product created event for product with id: {}, name: {}, price: {}, quantity: {}",
        productCreatedDTO.getProductId(),
        productCreatedDTO.getName(),
        productCreatedDTO.getPrice(),
        productCreatedDTO.getQuantity());
  }
}
