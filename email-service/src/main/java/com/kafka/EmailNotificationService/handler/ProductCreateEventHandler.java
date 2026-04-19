package com.kafka.EmailNotificationService.handler;

import com.kafka.EmailNotificationService.error.NonRetryableException;
import com.kafka.EmailNotificationService.error.RetryableException;
import com.kafka.core.ProductCreatedDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Component
@KafkaListener(topics = "product-created-events-topic", groupId = "product-created-events-v2")
public class ProductCreateEventHandler {
  private final Logger LOGGER = LoggerFactory.getLogger(ProductCreateEventHandler.class);
  private RestTemplate restTemplate;

  public ProductCreateEventHandler(RestTemplate restTemplate) {
    this.restTemplate = restTemplate;
  }

  @KafkaHandler
  public void handle(ProductCreatedDTO productCreatedDTO) {
    logInfoMessage(
        "Received product created event for product with id: {}, name: {}, price: {}, quantity: {}",
        productCreatedDTO);

    try {
      ResponseEntity<String> response =
          restTemplate.exchange("http://localhost:8082/response/200", HttpMethod.GET, null, String.class);
      if (response.getStatusCode().value() == HttpStatus.OK.value()) {
        logInfoMessage(
            "Successfully processed product created event for product with id: {}, name: {}, price: {}, quantity: {}",
            productCreatedDTO);
      }
    } catch (ResourceAccessException ex) {
      logErrorMessage(productCreatedDTO, ex);
      throw new RetryableException(ex);
    } catch (HttpServerErrorException ex) {
      logErrorMessage(productCreatedDTO, ex);
      throw new NonRetryableException(ex);
    } catch (Exception ex) {
      logErrorMessage(productCreatedDTO, ex);
      throw new NonRetryableException(ex);
    }
  }

  private void logInfoMessage(String s, ProductCreatedDTO productCreatedDTO) {
    LOGGER.info(
        s,
        productCreatedDTO.getProductId(),
        productCreatedDTO.getName(),
        productCreatedDTO.getPrice(),
        productCreatedDTO.getQuantity());
  }

  private void logErrorMessage(ProductCreatedDTO productCreatedDTO, Exception ex) {
    LOGGER.error(
        "Failed to process product created event for product with id: {}, name: {}, price: {}, quantity: {}. Error: {}",
        productCreatedDTO.getProductId(),
        productCreatedDTO.getName(),
        productCreatedDTO.getPrice(),
        productCreatedDTO.getQuantity(),
        ex.getMessage());
  }
}
