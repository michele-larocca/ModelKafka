package it.model.kafka.consumer.group;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerGroup {
	
	public void start(List<String> topics){
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092, localhost:9093");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("group.id", "test-group");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(topics);
		
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
