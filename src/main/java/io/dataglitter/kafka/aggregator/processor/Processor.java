package io.dataglitter.kafka.aggregator.processor;

/**
 * Created by reddys on 21/04/2018.
 */
public interface Processor<T1, T2, T3, T4> {

    T4 processStream(T1 kStreamBuilder, T2 readTopic, T3 writeTopic);
}
