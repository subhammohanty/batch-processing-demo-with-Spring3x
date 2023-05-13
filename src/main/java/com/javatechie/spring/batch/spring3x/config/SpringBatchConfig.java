package com.javatechie.spring.batch.spring3x.config;

import com.javatechie.spring.batch.spring3x.entity.Customer;
import com.javatechie.spring.batch.spring3x.listner.StepSkipListener;
import com.javatechie.spring.batch.spring3x.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.File;

@Configuration
@AllArgsConstructor
public class SpringBatchConfig {


    private CustomerRepository customerRepository;


    @Bean
    @StepScope // By default @Bean is Singleton to instanciate at every execution we annotate with @StepScope
    public FlatFileItemReader<Customer> reader(@Value("#{jobParameters[filePath]}") String pathToImport){
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource(new File(pathToImport)));
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());
        return itemReader;
    }

    //Old Implementation of static file read from location
//    @Bean
//    public FlatFileItemReader<Customer> reader(){
//        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
//        itemReader.setResource(new FileSystemResource("src/.../..."));
//        itemReader.setName("csvReader");
//        itemReader.setLinesToSkip(1);
//        itemReader.setLineMapper(lineMapper());
//        return itemReader;
//    }


    private LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob" , "age");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }

    @Bean
    public CustomerProcessor processor(){
        return new CustomerProcessor();
    }

    @Bean
    public RepositoryItemWriter<Customer> writer(){
        RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
        writer.setRepository(customerRepository);
        writer.setMethodName("save");
        return  writer;
    }

    @Bean
    public Step step1(FlatFileItemReader<Customer> reader , JobRepository jobRepository, PlatformTransactionManager transactionManager){
        return new StepBuilder("slaveStep" , jobRepository).<Customer , Customer>chunk(10, transactionManager)
                .reader(reader)
                .processor(processor())
                .writer(writer())
                .faultTolerant()
//                .skipLimit(1000)
//                .skip(NumberFormatException.class)
                .listener(skipListener())
                .skipPolicy(skipPolicy())
                .taskExecutor(taskExecutor()) //To run Asynchronously
                .build();
    }

    @Bean
    public Job runJob(FlatFileItemReader<Customer> reader, JobRepository jobRepository, PlatformTransactionManager transactionManager){
        return new JobBuilder("importCustomers", jobRepository)
                .flow(step1(reader,jobRepository, transactionManager))
                .end().build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(10);
        return asyncTaskExecutor;
    }

    @Bean
    public SkipPolicy skipPolicy(){
        return new ExceptionSkipPolicy();
    }

    @Bean
    public SkipListener skipListener(){
        return new StepSkipListener();
    }

}
