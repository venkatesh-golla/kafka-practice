package com.kafka.products.controller;

import com.kafka.products.dto.CreateProductRestModel;
import com.kafka.products.exception.ErrorMessage;
import com.kafka.products.service.ProductService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/products")
public class ProductController {
  ProductService productService;

  public ProductController(ProductService productService) {
    this.productService = productService;
  }

  @PostMapping
  public ResponseEntity<Object> createProduct(@RequestBody CreateProductRestModel product) {
    String productId = null;
    try {
      productId = productService.createProduct(product);
    } catch (Exception e) {
      e.printStackTrace();
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(new ErrorMessage(new Date(), e.getMessage(), "Error creating product"));
    }
    return ResponseEntity.status(HttpStatus.CREATED).body(productId);
  }
}
