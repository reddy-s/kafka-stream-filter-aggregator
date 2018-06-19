package io.dataglitter.kafka.aggregator.processor;

import io.dataglitter.kafka.aggregator.constant.TweetConstants;
import io.dataglitter.kafka.aggregator.processor.impl.ProcessorFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Created by reddys on 14/04/2018.
 */
@Component
public class TweetProcessor {

    Logger logger = LoggerFactory.getLogger(TweetProcessor.class);

    @Value("${kafka.topic.read}")
    private String readTopic;

    @Value("${kafka.topic.write}")
    private String writeTopic;

    @Value("${kafka.processor}")
    private String processorRequest;

    @Autowired
    private StreamsConfig kStreamConfig;

    private ProcessorFactory processorFactory;

    public TweetProcessor(ProcessorFactory processorFactory) {
        this.processorFactory = processorFactory;
    }

    @Bean
    @SuppressWarnings("unchecked")
    public KStream<String, GenericRecord> kStream(StreamsBuilder kStreamBuilder) throws Exception {
        Processor processor= processorFactory.getProcessor(processorRequest);
        return (KStream<String, GenericRecord>) processor.processStream(kStreamBuilder, readTopic, writeTopic);
    }

    @KafkaListener(id = "${kafka.consumer.checker-id}", topics = "${kafka.topic.write}")
    public void listen(GenericRecord record) throws Exception {
            logger.info(TweetConstants.TWEET_RECEIVED  + record.get(TweetConstants.ID) + TweetConstants.SEPERATOR +
                    TweetConstants.IS_RETWEET + TweetConstants.SPLIT + record.get(TweetConstants.IS_RETWEET).toString()
            );
    }
}
