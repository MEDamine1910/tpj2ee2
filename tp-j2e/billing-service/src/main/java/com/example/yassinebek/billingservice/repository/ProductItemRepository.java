package com.example.yassinebek.billingservice.repository;


import com.example.yassinebek.billingservice.entites.ProductItem;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductItemRepository extends JpaRepository<ProductItem, Long> {
}