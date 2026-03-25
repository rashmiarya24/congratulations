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


<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>batch-upsert-jdbc</artifactId>
    <version>1.0</version>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.2.5</version>
    </parent>

    <dependencies>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-batch</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-jdbc</artifactId>
        </dependency>

        <dependency>
            <groupId>com.mysql</groupId>
            <artifactId>mysql-connector-j</artifactId>
        </dependency>

        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
        </dependency>

    </dependencies>
</project>

spring.datasource.url=jdbc:mysql://localhost:3306/batch_db
spring.datasource.username=root
spring.datasource.password=root

spring.batch.jdbc.initialize-schema=always

input.folder.path=/Users/your-folder/gz-files

@SpringBootApplication
@EnableBatchProcessing
public class BatchApplication {
    public static void main(String[] args) {
        SpringApplication.run(BatchApplication.class, args);
    }
}

@Data
public class File1Model {
    private Long id;
    private String type;
    private String name;
    private Integer age;
}


@Data
public class File2Model {
    private Long productId;
    private String region;
    private String productName;
}


CREATE TABLE file1_table (
    id BIGINT,
    type VARCHAR(50),
    name VARCHAR(100),
    age INT,
    PRIMARY KEY (id, type)
);

CREATE TABLE file2_table (
    product_id BIGINT,
    region VARCHAR(50),
    product_name VARCHAR(100),
    PRIMARY KEY (product_id, region)
);

public class FileContextHolder {

    private static final ThreadLocal<String> context = new ThreadLocal<>();

    public static void set(String fileName) {
        context.set(fileName);
    }

    public static String get() {
        return context.get();
    }

    public static void clear() {
        context.remove();
    }
}

@Component
public class LineMapperFactory {

    public LineMapper<Object> getMapper(String fileName) {

        if (fileName.contains("file1")) return file1Mapper();
        if (fileName.contains("file2")) return file2Mapper();

        throw new RuntimeException("Unknown file: " + fileName);
    }

    private LineMapper<Object> file1Mapper() {

        DefaultLineMapper<Object> mapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("id", "type", "name", "age");

        BeanWrapperFieldSetMapper<Object> fsm = new BeanWrapperFieldSetMapper<>();
        fsm.setTargetType(File1Model.class);

        mapper.setLineTokenizer(tokenizer);
        mapper.setFieldSetMapper(fsm);

        return mapper;
    }

    private LineMapper<Object> file2Mapper() {

        DefaultLineMapper<Object> mapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("productId", "region", "productName");

        BeanWrapperFieldSetMapper<Object> fsm = new BeanWrapperFieldSetMapper<>();
        fsm.setTargetType(File2Model.class);

        mapper.setLineTokenizer(tokenizer);
        mapper.setFieldSetMapper(fsm);

        return mapper;
    }
}


@Component
@StepScope
public class MultiGzipReader extends MultiResourceItemReader<Object> {

    @Value("${input.folder.path}")
    private String folderPath;

    @Autowired
    private DynamicFlatFileReader delegate;

    @PostConstruct
    public void init() {

        File folder = new File(folderPath);

        Resource[] resources = Arrays.stream(folder.listFiles((d, n) -> n.endsWith(".gz")))
                .map(FileSystemResource::new)
                .toArray(Resource[]::new);

        this.setResources(resources);
        this.setDelegate(delegate);
    }
}


@Component
@StepScope
public class DynamicFlatFileReader extends FlatFileItemReader<Object> {

    @Autowired
    private LineMapperFactory mapperFactory;

    @Override
    public void setResource(Resource resource) {

        try {
            String fileName = resource.getFilename();

            FileContextHolder.set(fileName);

            InputStream gzip = new GZIPInputStream(resource.getInputStream());
            BufferedReader reader = new BufferedReader(new InputStreamReader(gzip));

            this.setBufferedReaderFactory((res, enc) -> reader);
            this.setLineMapper(mapperFactory.getMapper(fileName));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}


@Component
public class CommonProcessor implements ItemProcessor<Object, Object> {
    @Override
    public Object process(Object item) {
        return item;
    }
}

@Component
public class JdbcWriterFactory {

    @Autowired
    private DataSource dataSource;

    public JdbcBatchItemWriter<Object> getWriter(String fileName) {

        if (fileName.contains("file1")) return file1Writer();
        if (fileName.contains("file2")) return file2Writer();

        throw new RuntimeException("Unknown file: " + fileName);
    }

    private JdbcBatchItemWriter<Object> file1Writer() {

        JdbcBatchItemWriter<Object> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);

        writer.setSql("""
            INSERT INTO file1_table (id, type, name, age)
            VALUES (:id, :type, :name, :age)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                age = VALUES(age)
        """);

        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        writer.afterPropertiesSet();

        return writer;
    }

    private JdbcBatchItemWriter<Object> file2Writer() {

        JdbcBatchItemWriter<Object> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);

        writer.setSql("""
            INSERT INTO file2_table (product_id, region, product_name)
            VALUES (:productId, :region, :productName)
            ON DUPLICATE KEY UPDATE
                product_name = VALUES(product_name)
        """);

        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        writer.afterPropertiesSet();

        return writer;
    }
}

@Component
public class DynamicJdbcWriter implements ItemWriter<Object> {

    @Autowired
    private JdbcWriterFactory factory;

    private final Map<String, JdbcBatchItemWriter<Object>> cache = new HashMap<>();

    @Override
    public void write(Chunk<? extends Object> chunk) throws Exception {

        String fileName = FileContextHolder.get();

        JdbcBatchItemWriter<Object> writer =
                cache.computeIfAbsent(fileName, factory::getWriter);

        writer.write(chunk);
    }
}

@Configuration
public class StepConfig {

    @Bean
    public Step step(JobRepository jobRepository,
                     PlatformTransactionManager txManager,
                     MultiGzipReader reader,
                     CommonProcessor processor,
                     DynamicJdbcWriter writer) {

        return new StepBuilder("step", jobRepository)
                .<Object, Object>chunk(500, txManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }
}

@Configuration
public class JobConfig {

    @Bean
    public Job job(JobRepository jobRepository, Step step) {

        return new JobBuilder("upsert-job", jobRepository)
                .start(step)
                .build();
    }
}



id,type,name,age
1,Alice,John,25
2,Bob,Smith,30
3,Alice,Rahul,28
1,Alice,John Updated,26
2,Bob,Smith Updated,31


productId,region,productName
101,US,iPhone
102,IN,OnePlus
103,EU,Samsung
101,US,iPhone Updated
102,IN,OnePlus Updated

