// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.dynamo;

import aws.proserve.bcs.dr.lambda.annotation.Source;
import aws.proserve.bcs.dr.lambda.annotation.Target;
import aws.proserve.bcs.dr.util.Preconditions;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.jmespath.ObjectMapperSingleton;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.StreamsWorkerFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import dagger.Module;
import dagger.Provides;

import javax.annotation.Nullable;
import javax.inject.Singleton;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

/**
 * @apiNote default region is where this instance is running.
 */
@Singleton
@Module
class DbModule {
    static final String KINESIS_APP = "DRPDynamoDB-KinesisApp-";
    static final String INVALID_CHAR = "[^0-9a-zA-Z-_\\.]";

    @Provides
    @Singleton
    static AmazonDynamoDBStreams sourceStream(
            @Source String region,
            @Nullable @Source AWSCredentialsProvider credentialsProvider) {
        return AmazonDynamoDBStreamsClientBuilder
                .standard()
                .withCredentials(credentialsProvider)
                .withRegion(region)
                .build();
    }

    /**
     * @apiNote CloudWatch should refer to the default one (in the deploying region)
     */
    @Provides
    @Singleton
    static AmazonCloudWatch cloudWatch() {
        return AmazonCloudWatchClientBuilder.standard().build();
    }

    @Provides
    @Singleton
    ObjectMapper objectMapper() {
        return ObjectMapperSingleton.getObjectMapper();
    }

    @Provides
    @Singleton
    AWSSecretsManager secretsManager() {
        return AWSSecretsManagerClientBuilder.defaultClient();
    }

    /**
     * @apiNote StepFunctions should refer to the default one (in the deploying region)
     */
    @Provides
    @Singleton
    static AWSStepFunctions stepFunctions() {
        return AWSStepFunctionsClientBuilder.standard().build();
    }

    @Provides
    @Singleton
    static AmazonDynamoDB targetDynamoDB(@Target String region) {
        return AmazonDynamoDBClientBuilder.standard().withRegion(region).build();
    }

    @Provides
    @Singleton
    static Worker getWorker(
            @SourceTable String sourceTable,
            AmazonDynamoDBStreams sourceStream,
            ProcessorFactory processorFactory,
            AmazonDynamoDB targetDynamoDb,
            AmazonCloudWatch cloudWatch) {
        final String workerId;
        try {
            workerId = "DRPDynamoDB-KinesisWorker"
                    + "-" + InetAddress.getLocalHost().getCanonicalHostName()
                    + "-" + UUID.randomUUID();
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }

        final var streams = sourceStream.listStreams(new ListStreamsRequest()
                .withTableName(sourceTable)).getStreams();
        Preconditions.checkArgument(!streams.isEmpty(), "Stream is not enabled.");
        final var streamArn = streams.get(0).getStreamArn();
        final var rawName = KINESIS_APP + streamArn.replaceAll(INVALID_CHAR, "-");

        final var worker = StreamsWorkerFactory.createDynamoDbStreamsWorker(
                processorFactory,
                new KinesisClientLibConfiguration(
                        rawName.substring(0, Math.min(255, rawName.length())),
                        streamArn,
                        new DefaultAWSCredentialsProviderChain(),
                        workerId)
                        .withInitialPositionInStream(InitialPositionInStream.LATEST),
                new AmazonDynamoDBStreamsAdapterClient(sourceStream),
                targetDynamoDb,
                cloudWatch);
        processorFactory.setWorker(worker);
        return worker;
    }
}
