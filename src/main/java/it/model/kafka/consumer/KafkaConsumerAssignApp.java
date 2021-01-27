package it.model.kafka.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class KafkaConsumerAssignApp {

	public static void main(String[] args){
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092, localhost:9093");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		List<TopicPartition> partitions = new ArrayList<>();
		TopicPartition topicPart0 = new TopicPartition("my-topic", 0);
		TopicPartition partitionTopicPart2 = new TopicPartition("partition-topic", 2);
		
		partitions.add(topicPart0);
		partitions.add(partitionTopicPart2);
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.assign(partitions);
		
		try {
			while(true) {
				ConsumerRecords<String,String> records = consumer.poll(10);
				
				for(ConsumerRecord<String,String> record : records) {
					System.out.println(
						String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s", 
							record.topic(), record.partition(), record.offset(), record.key(), record.value()));
				}
			}
			
		} catch(Exception e){
			e.printStackTrace();
			
		} finally {
			consumer.close();
		}
		
	}
	
}
