@Component
public class MoveFileTasklet implements Tasklet {

    private final S3Client s3Client = S3Client.builder().build();

    @Override
    public RepeatStatus execute(StepContribution contribution,
                                ChunkContext chunkContext) {

        String bucket = "my-bucket";
        String sourceKey = "csv/file.csv";
        String destinationKey = "processed/file.csv";

        CopyObjectRequest copyRequest = CopyObjectRequest.builder()
                .copySource(bucket + "/" + sourceKey)
                .destinationBucket(bucket)
                .destinationKey(destinationKey)
                .build();

        s3Client.copyObject(copyRequest);

        DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(sourceKey)
                .build();

        s3Client.deleteObject(deleteRequest);

        return RepeatStatus.FINISHED;
    }
}