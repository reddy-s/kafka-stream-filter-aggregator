package io.dataglitter.kafka.aggregator.processor.impl;

import io.dataglitter.kafka.aggregator.constant.TweetConstants;
import io.dataglitter.kafka.aggregator.processor.Processor;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Created by reddys on 21/04/2018.
 */
public class ReTweetFilterProcessor implements Processor<StreamsBuilder, String, String, KStream<String, GenericRecord>> {

    @Override
    public KStream<String, GenericRecord> processStream(StreamsBuilder kStreamBuilder, String readTopic, String writeTopic) {
        KStream<String, GenericRecord> stream = kStreamBuilder.stream(readTopic);
        stream.filter((key, value) -> (boolean) value.get(TweetConstants.IS_RETWEET))
                .to(writeTopic);
        return stream;
    }
}
