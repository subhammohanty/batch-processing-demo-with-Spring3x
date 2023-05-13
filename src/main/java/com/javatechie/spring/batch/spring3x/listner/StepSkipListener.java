package com.javatechie.spring.batch.spring3x.listner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.javatechie.spring.batch.spring3x.entity.Customer;

import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;

public class StepSkipListener implements SkipListener<Customer, Number> {

    Logger logger = LoggerFactory.getLogger(StepSkipListener.class);

    @Override // item read
    public void onSkipInRead(Throwable throwable) {
        logger.info("A failure in read {} ", throwable.getMessage());
    }

    @Override // item write
    public void onSkipInWrite(Number item, Throwable throwable) {
        logger.info("A failure on write {} , {} ", throwable.getMessage(), item);
    }

    @SneakyThrows
    @Override //item process
    public void onSkipInProcess(Customer customer, Throwable throwable) {
        logger.info("Item {} was skipped due to he exception {}", new ObjectMapper().writeValueAsString(customer) , throwable.getMessage());
    }
}
