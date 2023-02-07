package org.example;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;

import java.net.URI;
import java.nio.file.Paths;
import java.util.List;
import java.util.ResourceBundle;
import java.util.concurrent.CompletableFuture;

public class Main {

    public static void main(String[] args) {
        System.out.println("Hello s3!");
        ResourceBundle rb = ResourceBundle.getBundle("app");
        AwsBasicCredentials credentials = AwsBasicCredentials.create(rb.getString("accessKey"), rb.getString("secretKey"));
        try {
            S3AsyncClient s3AsyncClient = S3AsyncClient.builder()
                    .region(Region.AP_NORTHEAST_1)
                    .endpointOverride(new URI(rb.getString("endPointUrl")))
                    .forcePathStyle(true)
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .build();
            String bucketName = rb.getString("bucketName");
            String dstPath = rb.getString("dstPath");
            String srcPath = rb.getString("srcPath");
            listBucketObjects(
                    s3AsyncClient,
                    bucketName
            );
            S3TransferManager transferManager = S3TransferManager.builder()
                    .s3Client(s3AsyncClient)
                    .build();
            uploadFile(transferManager, bucketName, dstPath, srcPath);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public static String uploadFile(S3TransferManager transferManager, String bucketName,
                             String key, String filePath) {
        UploadFileRequest uploadFileRequest =
                UploadFileRequest.builder()
                        .putObjectRequest(b -> b.bucket(bucketName).key(key))
                        .addTransferListener(LoggingTransferListener.create())
                        .source(Paths.get(filePath))
                        .build();

        FileUpload fileUpload = transferManager.uploadFile(uploadFileRequest);

        CompletedFileUpload uploadResult = fileUpload.completionFuture().join();
        return uploadResult.response().eTag();
    }

    public static void listBucketObjects(S3AsyncClient s3AsyncClient, String bucketName) {

        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .build();

            CompletableFuture<ListObjectsResponse> res = s3AsyncClient.listObjects(listObjects);
            List<S3Object> objects = res.get().contents();

            System.out.print("\n");
            for (S3Object myValue : objects) {
                System.out.print(myValue.key() + " (" + calKb(myValue.size()) + " KBs)\n");
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    //convert bytes to kbs.
    private static long calKb(Long val) {
        return val / 1024;
    }
}
