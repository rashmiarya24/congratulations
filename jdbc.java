CREATE TABLE my_table (
    id BIGINT PRIMARY KEY,
    name VARCHAR(100),
    amount DOUBLE
);

package com.example.batch.model;

public class MyEntity {

    private Long id;
    private String name;
    private Double amount;

    public MyEntity() {}

    public MyEntity(Long id, String name, Double amount) {
        this.id = id;
        this.name = name;
        this.amount = amount;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Double getAmount() { return amount; }
    public void setAmount(Double amount) { this.amount = amount; }
}


package com.example.batch.reader;

import com.example.batch.model.MyEntity;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.InputStreamResource;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

public class GzipReader {

    public static FlatFileItemReader<MyEntity> reader(String filePath) throws Exception {

        GZIPInputStream gzipInputStream =
                new GZIPInputStream(new FileInputStream(filePath));

        FlatFileItemReader<MyEntity> reader = new FlatFileItemReader<>();
        reader.setResource(new InputStreamResource(gzipInputStream));

        reader.setLineMapper((line, lineNumber) -> {
            String[] fields = line.split(",");

            return new MyEntity(
                    Long.parseLong(fields[0]),
                    fields[1],
                    Double.parseDouble(fields[2])
            );
        });

        return reader;
    }
}

package com.example.batch.config;

import com.example.batch.model.MyEntity;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSourceProvider;

import javax.sql.DataSource;

@Configuration
public class WriterConfig {

    @Bean
    public JdbcBatchItemWriter<MyEntity> writer(DataSource dataSource) {

        JdbcBatchItemWriter<MyEntity> writer = new JdbcBatchItemWriter<>();

        writer.setDataSource(dataSource);

        writer.setSql("""
            INSERT INTO my_table (id, name, amount)
            VALUES (:id, :name, :amount)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                amount = VALUES(amount)
        """);

        writer.setItemSqlParameterSourceProvider(
                new BeanPropertySqlParameterSourceProvider<>()
        );

        return writer;
    }
}

package com.example.batch.config;

import com.example.batch.model.MyEntity;
import com.example.batch.reader.GzipReader;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StepConfig {

    @Bean
    public Step step(StepBuilderFactory stepBuilderFactory,
                     WriterConfig writerConfig) throws Exception {

        return stepBuilderFactory.get("step1")
                .<MyEntity, MyEntity>chunk(1000)
                .reader(GzipReader.reader("data/input.csv.gz"))
                .writer(writerConfig.writer(null))
                .build();
    }
}

package com.example.batch.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.Step;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JobConfig {

    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory, Step step) {
        return jobBuilderFactory.get("myJob")
                .start(step)
                .build();
    }
}

package com.example.batch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
