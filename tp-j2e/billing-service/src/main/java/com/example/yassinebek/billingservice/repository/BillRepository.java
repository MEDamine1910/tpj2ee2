package com.example.yassinebek.billingservice.repository;

import com.example.yassinebek.billingservice.entites.Bill;
import org.springframework.data.jpa.repository.JpaRepository;

public interface BillRepository  extends JpaRepository<Bill, Long> {
}
