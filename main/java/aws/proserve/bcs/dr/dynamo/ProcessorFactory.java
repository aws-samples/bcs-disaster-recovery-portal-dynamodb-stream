// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.dynamo;

import aws.proserve.bcs.dr.lambda.annotation.TaskToken;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.SendTaskHeartbeatRequest;
import com.amazonaws.services.stepfunctions.model.TaskTimedOutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Singleton
class ProcessorFactory implements IRecordProcessorFactory {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String targetTable;
    private final AmazonDynamoDBStreams sourceStream;
    private final AmazonDynamoDB targetDynamoDb;
    private final AWSStepFunctions stepFunctions;
    private final String taskToken;
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    private Worker worker;

    @Inject
    ProcessorFactory(
            @TargetTable String targetTable,
            AmazonDynamoDBStreams sourceStream,
            AmazonDynamoDB targetDynamoDb,
            AWSStepFunctions stepFunctions,
            @Nullable @TaskToken String taskToken) {
        this.targetTable = targetTable;
        this.sourceStream = sourceStream;
        this.targetDynamoDb = targetDynamoDb;
        this.stepFunctions = stepFunctions;
        this.taskToken = taskToken;
        heartbeat();
    }

    private void heartbeat() {
        executorService.scheduleAtFixedRate(() -> {
            try {
                if (taskToken != null) {
                    stepFunctions.sendTaskHeartbeat(new SendTaskHeartbeatRequest()
                            .withTaskToken(taskToken));
                }
            } catch (TaskTimedOutException e) {
                if (e.getMessage().contains("Provided task does not exist anymore")) {
                    log.warn("Step functions execution is thought to be stopped", e);
                    shutdown().run();
                }
            }
        }, 1, 1, TimeUnit.HOURS);
    }

    void setWorker(Worker worker) {
        this.worker = worker;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new ItemProcessor(targetDynamoDb, targetTable, stepFunctions, taskToken, shutdown());
    }

    /**
     * @apiNote Must run in a separate thread, otherwise the record processor cannot properly shutdown.
     * <p>
     * Must not return singleton shutdown runnable to the record processor. While one thread is waiting for shutdown
     * gracefully, the other thread may complete the whole shutdown and interrupt the waiting.
     */
    private Runnable shutdown() {
        return () -> {
            final var thread = new Thread(
                    () -> {
                        log.info("Start shutdown gracefully.");
                        final var shutdownFuture = worker.startGracefulShutdown();
                        executorService.shutdown();

                        log.info("Wait up to one minute for shutdown to complete.");
                        try {
                            shutdownFuture.get(1, TimeUnit.MINUTES);
                        } catch (InterruptedException | ExecutionException | TimeoutException e) {
                            log.info("Unable to shutdown gracefully, thus shutdown directly.", e);
                            worker.shutdown();
                        }

                        try {
                            executorService.awaitTermination(1, TimeUnit.MINUTES);
                        } catch (InterruptedException e) {
                            executorService.shutdownNow();
                        }

                        log.info("Worker shutdown.");
                    });
            thread.setDaemon(true);
            thread.start();
        };
    }
}
