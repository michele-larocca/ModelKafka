package it.model.kafka.consumer.group;

import java.util.Arrays;
import java.util.List;

public class KafkaConsumerGroupApp {
	
	public static void main(String[] args){
		int consumers = 4;
		List<String> topics = Arrays.asList("my-topic", "partition-topic");
		
		for(int i=0; i<consumers; i++){
			KafkaConsumerGroup consumer = new KafkaConsumerGroup();
			consumer.start(topics);
		}
	}
}
