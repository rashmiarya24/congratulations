@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job csvImportJob(Step listFilesStep,
                            Step processFileStep,
                            Step moveFileStep) {

        return jobBuilderFactory.get("csvImportJob")
                .start(listFilesStep)
                .next(processFileStep)
                .next(moveFileStep)
                .build();
    }
}