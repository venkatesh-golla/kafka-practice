package com.kafka.EmailNotificationService.error;

public class NonRetryableException extends RuntimeException {
  public NonRetryableException(String message) {
    super(message);
  }

  public NonRetryableException(Throwable cause) {
    super(cause);
  }
}
