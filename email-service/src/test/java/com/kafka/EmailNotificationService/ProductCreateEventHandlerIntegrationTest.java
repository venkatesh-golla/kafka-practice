package com.kafka.EmailNotificationService;

import com.kafka.EmailNotificationService.entity.ProcessedEventEntity;
import com.kafka.EmailNotificationService.handler.ProductCreateEventHandler;
import com.kafka.EmailNotificationService.repository.ProcessedEventRepository;
import com.kafka.core.ProductCreatedDTO;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@EmbeddedKafka
@SpringBootTest(
    properties = "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}")
public class ProductCreateEventHandlerIntegrationTest {
  @MockBean ProcessedEventRepository processedEventRepository;
  @MockBean RestTemplate restTemplate;
  @Autowired KafkaTemplate<String, Object> kafkaTemplate;
  @SpyBean ProductCreateEventHandler productCreateEventHandler;

  @Test
  void testProductCreatedEventHandler_OnProductCreated_HandlesEvent()
      throws ExecutionException, InterruptedException {
    ProductCreatedDTO productCreatedDTO = new ProductCreatedDTO();
    productCreatedDTO.setProductId(UUID.randomUUID().toString());
    productCreatedDTO.setName("Test Product");
    productCreatedDTO.setPrice(new BigDecimal(10));
    productCreatedDTO.setQuantity(1);

    String messageId = UUID.randomUUID().toString();
    String messageKey = productCreatedDTO.getProductId();

    ProducerRecord<String, Object> producerRecord =
        new ProducerRecord<>("product-created-events-topic", messageKey, productCreatedDTO);
    producerRecord.headers().add("messageId", messageId.getBytes());
    producerRecord.headers().add(KafkaHeaders.RECEIVED_KEY, messageKey.getBytes());

    ProcessedEventEntity processedEventEntity = new ProcessedEventEntity();
    when(processedEventRepository.findByMessageId(anyString())).thenReturn(processedEventEntity);
    when(processedEventRepository.save(any(ProcessedEventEntity.class)))
        .thenReturn(processedEventEntity);
    String responseBody = "{\"key\":\"value\"}";
    HttpHeaders httpHeaders = new HttpHeaders();
    httpHeaders.setContentType(MediaType.APPLICATION_JSON);
    ResponseEntity<String> responseEntity =
        new ResponseEntity<>(responseBody, httpHeaders, HttpStatus.OK);
    when(restTemplate.exchange(anyString(), any(), any(), any(Class.class)))
        .thenReturn(responseEntity);

    kafkaTemplate.send(producerRecord).get();

    ArgumentCaptor<String> messageIdCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> messageKeyCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<ProductCreatedDTO> productCreatedDTOCaptor =
        ArgumentCaptor.forClass(ProductCreatedDTO.class);
    verify(productCreateEventHandler, timeout(5000).times(1))
        .handle(
            productCreatedDTOCaptor.capture(),
            messageIdCaptor.capture(),
            messageKeyCaptor.capture());
    assertEquals(
        productCreatedDTO.getProductId(), productCreatedDTOCaptor.getValue().getProductId());
    assertEquals(messageId, messageIdCaptor.getValue());
    assertEquals(messageKey, messageKeyCaptor.getValue());
  }
}
