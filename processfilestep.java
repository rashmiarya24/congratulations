@Bean
public Step processFileStep(FlatFileItemReader<User> reader,
                            ItemProcessor<User, User> processor,
                            JdbcBatchItemWriter<User> writer) {

    return stepBuilderFactory.get("processFileStep")
            .<User, User>chunk(1000)
            .reader(reader)
            .processor(processor)
            .writer(writer)
            .build();
}