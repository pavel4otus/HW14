package ru.pavel2107.otus.hw14.batch;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private final Logger logger = LoggerFactory.getLogger("Batch");


    @Autowired JobBuilderFactory jobBuilderFactory;

    @Autowired GenreStepConfig  genreStepConfig;
    @Autowired AuthorStepConfig authorStepConfig;
    @Autowired BookStepConfig   bookStepConfig;

    @Bean
    public Job migration(){
        return jobBuilderFactory.get( "migration2Mongo")
                .incrementer( new RunIdIncrementer())
                .start( genreStepConfig.stepGenre())
                .next(  authorStepConfig.stepAuthor())
                .next(  bookStepConfig.stepBook())
                .listener(new JobExecutionListener() {
                    @Override
                    public void beforeJob(JobExecution jobExecution) {
                        logger.info("Начало job");
                    }

                    @Override
                    public void afterJob(JobExecution jobExecution) {
                        logger.info("Конец job");
                    }
                })
                .build();
    }

}


