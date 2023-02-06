package org.example;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URI;
import java.util.List;
import java.util.ResourceBundle;
import java.util.concurrent.CompletableFuture;

public class Main {

    public static void main(String[] args) {
        System.out.println("Hello s3!");
        ResourceBundle rb = ResourceBundle.getBundle("app");

        listBucketObjects(
                rb.getString("accessKey"),
                rb.getString("secretKey"),
                rb.getString("endPointUrl"),
                rb.getString("bucketName")
        );
    }

    public static void listBucketObjects(String accessKey, String secretKey, String endPointUrl, String bucketName) {

        try {
            AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
            S3AsyncClient s3AsyncClient = S3AsyncClient.crtBuilder()
                    .region(Region.AP_NORTHEAST_1)
                    .endpointOverride(new URI(endPointUrl))
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .build();

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
