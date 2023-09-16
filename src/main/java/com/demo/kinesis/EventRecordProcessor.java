package com.demo.kinesis;

import com.demo.kinesis.model.CartAbandonmentEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Performs the act of processing CartAbandonmentEvent records from the Kinesis Data Stream implementing
 * logic to calculate the time of abandonment and average abandonment order size over 30-second time windows.
 */
public class EventRecordProcessor implements ShardRecordProcessor {
    static final Logger logger = LogManager.getLogger(EventRecordProcessor.class);

    private String shardId;
    private EventsManager eventsMgr = new EventsManager();

    static final long REPORTING_INTERVAL_MILLIS = 30000L;
    private long nextReportingTimeMillis;

    static final long MAX_RETRY_SLEEP_MILLIS = 640000L;
    static final long DEFAULT_RETRY_SLEEP_MILLIS = 10000L;
    private long retrySleepMs = DEFAULT_RETRY_SLEEP_MILLIS;

    static final long CART_ABANDONMENT_TIME_MINS = 30;

    private final ObjectMapper objMapper = new ObjectMapper();

    /**
     * RecordProcessor instance is being initialized to begin receiving records for a given shard.
     */
    @Override
    public void initialize(InitializationInput initializationInput) {
        shardId = initializationInput.shardId();
        logger.info(String.format("Initialized shard %s @ sequence %s",
                shardId, initializationInput.extendedSequenceNumber().toString()));
        nextReportingTimeMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
    }


    /**
     * The main logic implementing method which iterates over each batch of records pulled
     * from the Kinesis Data Stream, aggregating them with the EventsManager class and
     * initiating the calculation of cart items abandonment time as well average abandonment order size calculation
     * for every reporting interval as well as checkpointing progress on the stream of records to DynamoDB
     */
    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        logger.info(String.format("Given %d record(s) to process from shard %s",
                processRecordsInput.records().size(), shardId));

        // Process each Kinesis record in the batch.
        processRecordsInput.records().forEach(this::processRecord);

        // When the reporting interval has elapsed, use EventsManager to calculate the abandonment time per
        // customer and the average abandonment order size over the interval that just finished and log it.
        // Then, set the new reporting time to the next increment of the reporting interval.
        if (System.currentTimeMillis() > nextReportingTimeMillis) {
            analyzeEvents();
            resetEvents();
            nextReportingTimeMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;

            // Checkpointing after a given period of time implements at least once
            // processing semantics, trading off throughput for the potential to process
            // duplicate order records
            checkpoint(processRecordsInput.checkpointer());
        }
    }

    /**
     * Analyzes events in the EventsManager to calculate the average potential order size and logs the result.
     */
    private void analyzeEvents() {
        double avgPotentialOrderSize = eventsMgr
                .calculateAvgPotentialOrderSize(System.currentTimeMillis(), CART_ABANDONMENT_TIME_MINS);
        logger.info(String.format("Shard %s Average Potential Order $%.2f", shardId, avgPotentialOrderSize));
    }

    /**
     * Resets the EventsManager to prepare for the next reporting interval.
     */
    private void resetEvents() {
        eventsMgr = new EventsManager();
    }

    /**
     * Processes a single KinesisClientRecord, attempting to deserialize it into a CartAbandonmentEvent
     * and adds it to the EventsManager for further processing.
     *
     * @param record The KinesisClientRecord to be processed.
     */
    private void processRecord(KinesisClientRecord record) {
        byte[] arr = new byte[record.data().remaining()];
        record.data().get(arr);
        try {
            CartAbandonmentEvent event = objMapper.readValue(arr, CartAbandonmentEvent.class);
            eventsMgr.addEvent(event);
        } catch (IOException e) {
            logger.error(String.format("Shard %s failed to deserialize record. Record Data: %s", shardId,
                    new String(arr, StandardCharsets.UTF_8)), e);
        }
    }

    /**
     * Called when lease (the assignment of a specific shard to this specific RecordProcessor instance)
     * has been revoked. This method is useful for communicating this information to outside systems or
     * simply logging such information.
     */
    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        logger.info(String.format("Shard %s lease lost", shardId));
    }

    /**
     * Called when the end of a shard has been reached. You must checkpoint here in order to finalize
     * processing the shard's records as well as to begin processing records from it's child shards.
     */
    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        logger.info(String.format("Shard %s end reached, doing final checkpoint", shardId));
        checkpoint(shardEndedInput.checkpointer());
    }

    /**
     * Scheduler has initiated shutdown and is giving RecordProcessor instance notice.
     */
    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        logger.info(String.format("Shard %s Scheduler is shutting down, checkpointing", shardId));
        checkpoint(shutdownRequestedInput.checkpointer());
    }

    /**
     * Performs checkpointing of the shard's progress, handling various exceptions and retries.
     *
     * @param checkpointer The RecordProcessorCheckpointer for checkpointing progress.
     */
    private void checkpoint(RecordProcessorCheckpointer checkpointer) {
        logger.info(String.format("Checkpointing shard %s", shardId));
        try {
            checkpointer.checkpoint();
        } catch (ShutdownException se) {
            // Ignore checkpoint if the processor instance has been shutdown (fail over).
            logger.info("Caught shutdown exception, skipping checkpoint.", se);
        } catch (ThrottlingException e) {
            logger.error("Throttling exception occurred", e);

            // Handle throttling exceptions and implement a backoff and retry policy.
            if (retrySleepMs < MAX_RETRY_SLEEP_MILLIS) {
                try {
                    Thread.sleep(retrySleepMs);
                    retrySleepMs = retrySleepMs * 2;
                    checkpoint(checkpointer);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                    logger.warn(String.format("Retry thread interrupted for shard %s and sleep %d", shardId, retrySleepMs));
                }
            } else {
                logger.error(String.format("Shard %s failed to perform checkpoint after backoff and retries exhausted", shardId));
                Runtime.getRuntime().halt(1);
            }
        } catch (InvalidStateException e) {
            // Handle InvalidStateException, which indicates an issue with the DynamoDB table.
            // Insufficient provisioned IOPS might be the reason
            logger.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        }
        retrySleepMs = DEFAULT_RETRY_SLEEP_MILLIS;
    }
}
