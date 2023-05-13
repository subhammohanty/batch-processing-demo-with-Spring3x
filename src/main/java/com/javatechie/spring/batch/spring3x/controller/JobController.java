package com.javatechie.spring.batch.spring3x.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;

@RestController
@RequestMapping("/jobs")
public class JobController {

    private final String TEMP_STORAGE = "C:/Users/submohan4/Desktop/Spring_BatchProcessing_Demo/";

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job job;


    @PostMapping("/importCustomers")
    public void importCsvToDBJob(@RequestParam("file") MultipartFile multipartFile) throws IOException {

        String originalPath = multipartFile.getOriginalFilename();
        File fileToImport = new File(TEMP_STORAGE+originalPath);
        //copy to temp location.
        multipartFile.transferTo(fileToImport);

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("filePath", TEMP_STORAGE+originalPath)
                .addLong("startAt" , System.currentTimeMillis()).toJobParameters();
        try {
            jobLauncher.run(job , jobParameters);
        } catch (JobExecutionAlreadyRunningException | JobParametersInvalidException | JobInstanceAlreadyCompleteException | JobRestartException e) {
            e.printStackTrace();
        }
    }
}
