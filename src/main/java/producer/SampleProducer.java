package producer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Scanner;

public class SampleProducer{

public static void main(String[] args) {
	
	
	final String BOOTSTRAP_SERVERS = "localhost:9092";
	//создание свойств продюссера
	Properties properties = new Properties();
	properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
	properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	
//	создание самого продюсера
	KafkaProducer<String,String> first_producer = new KafkaProducer<String, String>(properties);  

	//создание записи (message)
	ProducerRecord<String, String> record=new ProducerRecord<String, String>("my-first", "Hye Kafka");  
	//отправляем запись
	
	first_producer.send(record, (Callback) new ProdCallBack()) ;

	System.out.println("+++++++++++++++++++++++++++++++++++++++++++++");
	first_producer.flush();
	first_producer.close();
	
}
	
}

class ProdCallBack implements Callback {
	 
//    private final long startTime;
//    private final int key;
//    private final String message;
// 
//    public DemoCallBack(long startTime, int key, String message) {
//        this.startTime = startTime;
//        this.key = key;
//        this.message = message;
//    }
// 
    /**
     * onCompletion method will be called when the record sent to the Kafka Server has been acknowledged.
     * 
     * @param metadata  The metadata contains the partition and offset of the record. Null if an error occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
	public void onCompletion(RecordMetadata recordMetadata, Exception e) {  
        Logger logger=LoggerFactory.getLogger(SampleProducer.class);  
        if (e== null) {  
            System.out.println("Successfully received the details as: \n" +  
                    "Topic:" + recordMetadata.topic() + "\n" +  
                    "Partition:" + recordMetadata.partition() + "\n" +  
                    "Offset" + recordMetadata.offset() + "\n" +  
                    "Timestamp" + recordMetadata.timestamp());  
                      }  
  
         else {  
            logger.error("Can't produce,getting error",e);  
  
        }  
    }  
}

