// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.dynamo;

import aws.proserve.bcs.dr.lambda.util.Assure;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.streamsadapter.StreamsRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.SendTaskHeartbeatRequest;
import com.amazonaws.services.stepfunctions.model.TaskTimedOutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Do not declare singleton here as one separate record processor for one shard. Manage the lifecycle of record
 * processor separately.
 */
class ItemProcessor extends StreamsRecordProcessor {
    private static final int RETRY = 10;
    private static final int BACKOFF_TIME_SECONDS = 3;
    private static final long CHECKPOINT_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1);

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final AmazonDynamoDB dynamoDB;
    private final String tableName;
    private final AWSStepFunctions stepFunctions;
    private final String taskToken;
    private final Runnable shutdown;

    private String shardId;
    private long nextCheckpointTimeInMillis;

    ItemProcessor(
            AmazonDynamoDB dynamoDB,
            String tableName,
            AWSStepFunctions stepFunctions,
            String taskToken,
            Runnable shutdown) {
        this.dynamoDB = dynamoDB;
        this.tableName = tableName;
        this.stepFunctions = stepFunctions;
        this.taskToken = taskToken;
        this.shutdown = shutdown;
    }

    @Override
    public void initialize(InitializationInput input) {
        this.shardId = input.getShardId();
        log.info("Shard [{}]: initialize", shardId);
    }

    @Override
    public void processStreamsRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        log.info("Shard [{}]: processes {} records", shardId, records.size());
        for (var record : records) {
            Assure.assure(() -> process(record), RETRY, BACKOFF_TIME_SECONDS);
        }

        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
    }

    private void process(Record record) {
        switch (record.getEventName()) {
            case "INSERT":
            case "MODIFY":
                modify(record);
                break;

            case "REMOVE":
                remove(record);
                break;
        }
    }

    private void modify(Record record) {
        dynamoDB.putItem(new PutItemRequest()
                .withTableName(tableName)
                .withItem(record.getDynamodb().getNewImage()));
    }

    private void remove(Record record) {
        dynamoDB.deleteItem(new DeleteItemRequest()
                .withTableName(tableName)
                .withKey(record.getDynamodb().getKeys()));
    }

    @Override
    public void shutdown(ShutdownInput input) {
        log.info("Shard [{}]: shutdown", shardId);

        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (input.getShutdownReason() == ShutdownReason.TERMINATE) {
            checkpoint(input.getCheckpointer());
        }
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        log.info("Shard [{}]: checkpoint", shardId);

        for (int i = 0; i < RETRY; i++) {
            try {
                checkpointer.checkpoint();

                if (taskToken != null) {
                    stepFunctions.sendTaskHeartbeat(new SendTaskHeartbeatRequest()
                            .withTaskToken(taskToken));
                }
                break;
            } catch (TaskTimedOutException e) {
                if (e.getMessage().contains("Provided task does not exist anymore")) {
                    log.warn("Step functions execution is thought to be stopped", e);
                    shutdown.run();
                }
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                log.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (RETRY - 1)) {
                    log.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    log.info("Transient issue when checkpointing - attempt " + (i + 1) + " of " + RETRY, e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                log.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }

            try {
                Thread.sleep(BACKOFF_TIME_SECONDS);
            } catch (InterruptedException e) {
                log.debug("Interrupted sleep", e);
            }
        }
    }
}
