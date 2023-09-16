package com.demo.kinesis;

import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

/**
 * The {@code EventRecordProcessorFactory} class is responsible for creating instances of the
 * {@link EventRecordProcessor} class, which are used as record processors for Kinesis shards.
 *
 * <p>It implements the {@link ShardRecordProcessorFactory} interface, providing a method to
 * create new instances of the {@link EventRecordProcessor}.
 *
 * <p>This factory is used by the Kinesis Scheduler to lease record processors for processing
 * data from individual shards within a Kinesis Data Stream.
 */
public class EventRecordProcessorFactory implements ShardRecordProcessorFactory {
    @Override
    public ShardRecordProcessor shardRecordProcessor() {
        return new EventRecordProcessor();
    }
}
