package com.kafka.products.service;

import com.kafka.products.dto.CreateProductRestModel;

import java.util.concurrent.ExecutionException;

public interface ProductService {
    String createProduct(CreateProductRestModel product) throws ExecutionException, InterruptedException;
}
