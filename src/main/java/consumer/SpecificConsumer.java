package consumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;  
import org.apache.kafka.clients.consumer.ConsumerRecords;  
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;  
  
  
import java.time.Duration;  
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;  
public class SpecificConsumer {
	
	public static void main(String[] args) {
		
		String bootstrapServers="localhost:9092";  
        String grp_id="third_app";  
        String topic="my-first";  
        //Creating consumer properties  
        Properties properties=new Properties();  
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);  
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());  
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());  
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,grp_id);  
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
//        TopicPartition part = new TopicPartition(topic, 2) ;
        
        //creating consumer  
       
	

	TopicPartition tp = new TopicPartition(topic, 0);

	try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
	    consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
	        @Override
	        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

	        @Override
	        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
	            // Move to the desired start offset 
	            consumer.seek(tp, 93L);
	        }
	    });
//	    boolean run = true;
//	    long lastOffset = 96L;
	    while (true) {
	        ConsumerRecords<String, String> crs = consumer.poll(Duration.ofMillis(1L));
	        for (ConsumerRecord<String, String> record : crs) {
	            System.out.println(record);
	        }
	    }
	}

}
}
//while(true){  
//    ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(1));  
//    for(ConsumerRecord<String,String> record: records){                      
//        System.out.println("Key: "+ record.key() + ", Value:" +record.value());  
//        System.out.println("Partition:" + record.partition()+",Offset:"+record.offset());  
//    }  
//} 
