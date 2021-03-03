/************************************************************
 *                  Package Import                          *
 ************************************************************* */
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/*******************Producer Program**************************** */
public class ProducerClass {
    private static ProducerRecord<String, String> myRandomJsonDataGenerator() {
        String keyed = "dummy";                                         //dummy key. sent along with the generated value, if required, I will replace it with real key as per the user requirement
        // The below statement creates an empty json {}
        ObjectNode recordvalue = JsonNodeFactory.instance.objectNode(); // creates {} => whole record
        ObjectNode metadata = JsonNodeFactory.instance.objectNode();    // creates {} => metadata
        ObjectNode eventdata = JsonNodeFactory.instance.objectNode();   // creates {} => event

        /***********For Unit test case, uncomment this****************/
        //Integer number = ThreadLocalRandom.current().nextInt(0, 3);
        Integer number = ThreadLocalRandom.current().nextInt(0, 50);    // Generates random number between 0 to 50 for userId and comment this line for Unit testing
        Integer eventSelector = ThreadLocalRandom.current().nextInt(0, 2); // Generates random number between 0,1,2 to select an event randomly in the event array
        Integer otherRandom = ThreadLocalRandom.current().nextInt(0, 10); //Generates random number between 0 to 10
        String[] event = new String[] {"event","process","trigger","checksum"}; // event array
        recordvalue.put("userId","userId" + number.toString()); //{userId:'userId1'}
        recordvalue.put("type",event[eventSelector]);       //{userId:'userId1',type:'trigger'}
        metadata.put("messageId","123sfdafas");          //{messageId:'123sfdafas'}
        //used the current time in millisecond as the value for sentAt and Timestamp
        metadata.put("sentAt", String.valueOf(Instant.now().toEpochMilli())); //{messageId:'123sfdafas',sentAt:1658569}
        metadata.put("timestamp", String.valueOf(Instant.now().toEpochMilli())); //{messageId:'123sfdafas',sentAt:1658569, timestamp:1658569}
        metadata.put("receivedAt",eventSelector.toString());    //{messageId:'123sfdafas',sentAt:'1658569', timestamp:'1658569',receivedAt:0}
        metadata.put("apikey","apikey" + otherRandom.toString());   //{messageId:'123sfdafas',sentAt:'1658569', timestamp:'1658569',receivedAt:0,apikey:'apikey5'}
        metadata.put("spaceId","space" + otherRandom.toString()); //{messageId:'123sfdafas',sentAt:'1658569', timestamp:'1658569',receivedAt:0,apikey:'apikey5',spaceId:'spaceId5'}
        metadata.put("version","v1"); //{messageId:'123sfdafas',sentAt:'1658569', timestamp:'1658569',receivedAt:0,apikey:'apikey5',spaceId:'spaceId5',version:'v1'}
        recordvalue.set("metadata",metadata);       // {userId:'userId1',type:'trigger',metadata:{messageId:'123sfdafas',sentAt:'1658569', timestamp:'1658569',receivedAt:0,apikey:'apikey5',spaceId:'spaceId5',version:'v1'}}
        recordvalue.put("event","Played Movie");    // {userId:'userId1',type:'trigger',metadata:{messageId:'123sfdafas',sentAt:'1658569', timestamp:'1658569',receivedAt:0,apikey:'apikey5',spaceId:'spaceId5',version:'v1'},event:'Played Movie'}
        eventdata.put("MovieID","MIM"+ otherRandom + "ddd" + eventSelector); // {MovieId:'MIM5ddd2'}
        recordvalue.set("eventData",eventdata);     //  {userId:'userId1',type:'trigger',metadata:{messageId:'123sfdafas',sentAt:'1658569', timestamp:'1658569',receivedAt:0,apikey:'apikey5',spaceId:'spaceId5',version:'v1'},event:'Played Movie', eventData:{MovieId:'MIM5ddd2'}}
        return new ProducerRecord<>("input_topic", keyed, recordvalue.toString());
    }

    public static void main(String[] args) {
        final Logger log = LoggerFactory.getLogger(ProducerClass.class);        //Log - slf4J
        int count;
        String bootstrap = "127.0.0.1:9092";            // We can call brokers as Bootstrap Servers. localhost:9092 is used to connect to the brokers.
        String ackConfiguration = "all";
        String retriesOnFailure="3";
        // Properties class is used to bundle all the properties as one and argument it to the producer
        Properties producerConfiguration = new Properties();
        // Adding configuration for kafka bootstrap server. Equivalent to --bootstrap-server localhost:9092
        producerConfiguration.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap);
        //Adding Key Serializer configuration to the Producer.
        producerConfiguration.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Adding Value Serializer configuration to the Producer.
        producerConfiguration.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Adding Ack configuration to the Producer. If the kafka acquires acknowledgement from all the broker, we can be sure that data wont be lost.
        producerConfiguration.setProperty(ProducerConfig.ACKS_CONFIG,ackConfiguration); // Highest guarantee that the data is progressed/available in all secondary node/replicas along with leader.
        producerConfiguration.setProperty(ProducerConfig.RETRIES_CONFIG,retriesOnFailure);// Number of times that the producer should try to send the same data again if it encounters an issue.
        producerConfiguration.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // Processes the data exactly-Once. Useful in banking transactions where duplicate records can cause business problems.
        count = 0;
        Producer<String, String> eventWriter = new KafkaProducer<>(producerConfiguration); // create a producer client that sends records/data to kafka

       while(true){                // Infinite loop/ recursive loop
            try{                    // handle exception raised by keyboard interrupt
            //send the producer record returned by the function myRandomJsonDataGenerator()
            eventWriter.send(myRandomJsonDataGenerator(), (recordMetadata, e) -> {     // callback which executes once the record is returned
            if (e != null){                 // if there are errors, it will be logged.
                log.error("The Error is",e);
            }
        });
        Thread.sleep(200);      // wait for 200 milliseconds before sending another record. If required,please remove this, but accessing the result will be difficult
            } catch(Exception e){
                break;                  // If exception occurs, break from the loop
            }
            System.out.println("Data is being produced...Please check the consumer");   // notifies us that the producer is generating records
        }
        /************************* for Unit test, Uncomment this section********************
        while(count<10){
            try{                    // handle exception raised by keyboard interrupt
                //send the producer record returned by the function myRandomJsonDataGenerator()
                eventWriter.send(myRandomJsonDataGenerator(), (recordMetadata, e) -> {     // callback which executes once the record is returned
                    if (e != null){                 // if there are errors, it will be logged.
                        log.error("The Error is",e);
                    }
                });count ++;
                Thread.sleep(200);      // wait for 200 milliseconds before sending another record. If required,please remove this, but accessing the result will be difficult
            } catch(Exception e){
                break;                  // If exception occurs, break from the loop
            }
            System.out.println("Data is being produced...Please check the consumer");
        }*/
        eventWriter.close();        // close the producer once the write is finished.
        Runtime.getRuntime().addShutdownHook(new Thread(eventWriter::close));   //Allows jvm to shutdown gracefully incase of interruptions/errors. hook which makes the background thread to close while jvm shuts down. it is a basic operation that belongs to java and not of streams.
    }
}
