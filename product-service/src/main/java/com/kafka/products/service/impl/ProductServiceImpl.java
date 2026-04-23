package com.kafka.products.service.impl;

import com.kafka.core.ProductCreatedDTO;
import com.kafka.products.dto.CreateProductRestModel;
import com.kafka.products.service.ProductService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static com.kafka.products.config.KafkaConfig.PRODUCT_CREATED_EVENTS_TOPIC;

@Service
public class ProductServiceImpl implements ProductService {
  private final Logger log = LoggerFactory.getLogger(ProductServiceImpl.class);
  KafkaTemplate<String, ProductCreatedDTO> kafkaTemplate;

  public ProductServiceImpl(KafkaTemplate<String, ProductCreatedDTO> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  @Override
  public String createProduct(CreateProductRestModel product)
      throws ExecutionException, InterruptedException {
    String productId = java.util.UUID.randomUUID().toString();
    ProductCreatedDTO productCreatedDTO =
        new ProductCreatedDTO(
            productId, product.getName(), product.getPrice(), product.getQuantity());

    //        CompletableFuture<SendResult<String,ProductCreatedDTO>> future=
    //                kafkaTemplate.send(PRODUCT_CREATED_EVENTS_TOPIC, productId,
    // productCreatedDTO);
    //        future.whenComplete((result, ex) -> {
    //            if (ex != null) {
    //                log.error("Failed to send message: " + ex.getMessage());
    //            } else {
    //                log.info("Message sent successfully: " +
    // result.getRecordMetadata().toString());
    //            }
    //        });
    //        future.join();
    log.info("Sending message to Kafka topic: " + PRODUCT_CREATED_EVENTS_TOPIC);

    ProducerRecord<String, ProductCreatedDTO> record =
        new ProducerRecord<>(PRODUCT_CREATED_EVENTS_TOPIC, productId, productCreatedDTO);
    byte[] messageIdBytes = UUID.randomUUID().toString().getBytes();
    record.headers().add("messageId", messageIdBytes);

    SendResult<String, ProductCreatedDTO> result = kafkaTemplate.send(record).get();

    log.info(
        "Message sent successfully: {} with productId: {} and messageId: {}",
        result.getRecordMetadata().toString(),
        productId,
        new String(messageIdBytes));

    log.info(
        "Partition: {}, Offset: {}, Topic: {}",
        result.getRecordMetadata().partition(),
        result.getRecordMetadata().offset(),
        result.getRecordMetadata().topic());

    return productId;
  }
}
