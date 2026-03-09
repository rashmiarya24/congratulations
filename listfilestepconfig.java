@Bean
public Step listFilesStep(S3ListFilesTasklet tasklet) {
    return stepBuilderFactory.get("listFilesStep")
            .tasklet(tasklet)
            .build();
}