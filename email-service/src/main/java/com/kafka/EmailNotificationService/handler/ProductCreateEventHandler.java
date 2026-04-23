package com.kafka.EmailNotificationService.handler;

import com.kafka.EmailNotificationService.entity.ProcessedEventEntity;
import com.kafka.EmailNotificationService.error.NonRetryableException;
import com.kafka.EmailNotificationService.error.RetryableException;
import com.kafka.EmailNotificationService.repository.ProcessedEventRepository;
import com.kafka.core.ProductCreatedDTO;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Component
@KafkaListener(topics = "product-created-events-topic", groupId = "product-created-events-v2")
public class ProductCreateEventHandler {
  private final Logger LOGGER = LoggerFactory.getLogger(ProductCreateEventHandler.class);
  private RestTemplate restTemplate;
  private ProcessedEventRepository processedEventRepository;

  public ProductCreateEventHandler(
      RestTemplate restTemplate, ProcessedEventRepository processedEventRepository) {
    this.restTemplate = restTemplate;
    this.processedEventRepository = processedEventRepository;
  }

  @KafkaHandler
  @Transactional
  public void handle(
      @Payload ProductCreatedDTO productCreatedDTO,
      @Header("messageId") String messageId,
      @Header(KafkaHeaders.RECEIVED_KEY) String messageKey) {
    logInfoMessage(
        "Received product created event for product with id: {}, name: {}, price: {}, quantity: {}",
        productCreatedDTO);

    // Check if the event has already been processed to achieve idempotency
    ProcessedEventEntity existingRecord = processedEventRepository.findByMessageId(messageId);
    if (existingRecord != null) {
      logInfoMessage(
          "Event with messageId: "
              + messageId
              + " has already been processed. Skipping processing for product with id: {}, name: {}, price: {}, quantity: {}",
          productCreatedDTO);
      return;
    }

    try {
      ResponseEntity<String> response =
          restTemplate.exchange(
              "http://localhost:8082/response/200", HttpMethod.GET, null, String.class);
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

    // Save processed event to the database to avoid re-processing in case of retries to achieve
    // idempotency
    try {
      processedEventRepository.save(
          new ProcessedEventEntity(messageId, productCreatedDTO.getProductId()));
    } catch (DataIntegrityViolationException ex) {
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
