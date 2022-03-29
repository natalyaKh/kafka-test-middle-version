package producer;

public class KafkaProducerDemo {
	 public static final String TOPIC = "testTopic";
     
	    public static void main(String[] args) {
	        boolean isAsync = false;
	        SampleProducer1 producerThread = new SampleProducer1(TOPIC, isAsync);
	        // start the producer
	        producerThread.run();
	 
	    }
}
