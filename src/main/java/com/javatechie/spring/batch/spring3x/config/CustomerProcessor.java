package com.javatechie.spring.batch.spring3x.config;

import com.javatechie.spring.batch.spring3x.entity.Customer;
import org.springframework.batch.item.ItemProcessor;

public class CustomerProcessor implements ItemProcessor<Customer , Customer> {

    @Override
    public Customer process(Customer customer) throws Exception {

        int age = Integer.parseInt(customer.getAge());
        if(age >= 18){
            return customer;
        }else{
            return null;
        }
//        return customer;
    }
}
