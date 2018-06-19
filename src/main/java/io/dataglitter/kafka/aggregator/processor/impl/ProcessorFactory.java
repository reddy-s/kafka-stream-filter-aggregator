package io.dataglitter.kafka.aggregator.processor.impl;

import io.dataglitter.kafka.aggregator.constant.TweetConstants;
import io.dataglitter.kafka.aggregator.processor.Processor;
import org.springframework.stereotype.Component;

/**
 * Created by reddys on 21/04/2018.
 */
@Component
public class ProcessorFactory {

    public Processor getProcessor(String processor) {
        if (processor == TweetConstants.RE_TWEET_FILTER_PROCESSOR) {
            return new ReTweetFilterProcessor();
        }
        return new ReTweetFilterProcessor();
    }
}
