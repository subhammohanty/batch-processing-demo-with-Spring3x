package com.javatechie.spring.batch.spring3x.repository;

import com.javatechie.spring.batch.spring3x.entity.Customer;
import org.springframework.data.jpa.repository.JpaRepository;


public interface CustomerRepository extends JpaRepository<Customer , Integer> {
}
