/************************************************************
 *                  Package Import                          *
************************************************************* */
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.time.Instant;
import java.util.Properties;

/**************Logic*******************/
/*  firstSeen = A user's earliest record/ user's time when he saw their first ever movie
*   lastSeen = A user's latest record/ user's time when he saw the latest movie
*   group By userId*/
/*******************Main Streams Program**************************** */
// t represents first run at time 0.
public class StreamsClass {
    private static JsonNode Balance(JsonNode event, JsonNode intermediate) { // Balance function specified in aggregate method. Return type JsonNode. <accessmodifiers> <static/abstract> <return type> <function(args)>
        // no need to create an object to call static method.
        ObjectNode newObject = JsonNodeFactory.instance.objectNode(); // create an empty json node. {}
        Long firstSeenValue = intermediate.get("firstSeen").asLong(); // at t=0, firstSeenValue=0, else firstSeenValue = minimum/earliest timestamp for a key.
        Long lastSeenValue = intermediate.get("lastSeen").asLong(); // at t=0, lastSeenValue=0, else lastSeenValue = highest/recent timestamp for a key.
        System.out.println(event.get("metadata").get("timestamp").asLong());   //For debugging purposes
        Long newBalanceInstant;     // Initializing newBalanceInstant
        if (firstSeenValue == 0){               //At t=0, firstSeen = max(0,currentTimestamp) -> firstSeen @ t0 = currentTimestamp
            newBalanceInstant = Math.max(firstSeenValue ,event.get("metadata").get("timestamp").asLong());
        }else{                                  //else  firstSeen = min(previousTimestamp,currentTimestamp)
            newBalanceInstant = Math.min(firstSeenValue ,event.get("metadata").get("sentAt").asLong());
        }
        Long newBalanceInstant1 = Math.max(lastSeenValue ,event.get("metadata").get("timestamp").asLong()); // acquired timestamp from incoming event
        newObject.put("userId", event.get("userId").toString());    //{userId:'userId1'}
        newObject.put("firstSeen", newBalanceInstant);              //{userId:'userId1',firstSeen:'1648526'}
        newObject.put("lastSeen", newBalanceInstant1);              //{userId:'userId1',firstSeen:'1648526',lastSeen:'1754862'}
        return newObject;
    }

    public static void main(String[] args) {
        String streamConsumer = "myStreamReader";
        String bootstrapServer = "127.0.0.1:9092";  // We can call brokers as Bootstrap Servers. localhost:9092 is used to connect to the brokers. If we connect to just one broker, we are automatically connected to the entire cluster.
        String offsetConfiguration = "earliest";    // Streams use kafka consumer client to read data from partitions
                                                    // This configuration will notify Stream to read from the start everytime the program runs.

        Properties streamConfiguration = new Properties();
        streamConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, streamConsumer); //ApplicationIds are like consumer groupIds. Each kafka partition can only be read by one consumer.
        streamConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        streamConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetConfiguration);

        // Exactly once processing - Idempotence. Prevents duplicate records while processing
        streamConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE); //Exactly once is a fantastic operation and fixed gaps in real time streaming.
        // Idempotence makes sure no data will be written twice to the cluster. It is extremely useful when we deal with sensitive bank transactions. If duplicate transactions(withdrawals) are recorded, the customer might lose his money. So idempotence is very important.
        // If producer hasn't received any acknowledge of data receipt, it sends the same record twice. The idempotence enables kafka to check whether the data has been already processed.
        // If the data was processed, it prevents duplication of data rather sends an acknowledgement back to client/producer.

        // Json Serde - Serializer and Deserializer to read/perform operations on Json
        // Created own Serdes using Kafka's serializer & Deserializer class along with JacksonBind
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        //Created a StreamBuilder - Like spark object, it gives access to various functions and operators
        KStreamBuilder myStreamBuilder = new KStreamBuilder();

        //Kstream is similar to a log which can have unbound inserts. It is very helpful while acquiring real-time data from kafka
        //With KStreams, we can perform stateless operations, like map(),flatmap() which does not depend on previous values
        KStream<String, JsonNode> inputStream =myStreamBuilder.stream(Serdes.String(), jsonSerde, "input_topic");

        //Ktables comes into play when we want to perform stateful operations like aggregate(), reduce() or count(),etc which requires the key's previous state/values.
        //If I group by key and count the number of records generated per user, I need to know how many records were processed before for that particular user
        //Hence, we use KTables. We can consider KTables like a Database Table.
        KTable<String, JsonNode> dataProcessor = inputStream
                // The input stream has dummy key and Json value. The below function will extract the appropriate key from the value and ignores the dummy key.
                .selectKey((key,value)->value.get("userId").toString())     // (dummy,{userId,event,metadata...}) --> (userId, {userId,event, metadata...})
                // It groups the data by key before aggregation
                .groupByKey(Serdes.String(), jsonSerde)         // (userid1,[{event1,event2,event3..}]) - presented for understanding purposes.
                // aggregate takes an initial value, add/operate/update with incoming data to produce intermediate results. The intermediate results will be updated until it reaches the final result.
                .aggregate(                                     //stateful operation
                        () -> { //Initializer
                            ObjectNode initialAggregatorValue = JsonNodeFactory.instance.objectNode(); // creating a empty json with value {}.
                            initialAggregatorValue.put("firstSeen", Instant.ofEpochMilli(0L).toString()); // {"firstSeen":"0"}
                            initialAggregatorValue.put("lastSeen", Instant.ofEpochMilli(0L).toString()); // {"firstSeen":"0","lastSeen":"0"}
                            return initialAggregatorValue;},            // returns initial value to perform operations
                        //calling a function.
                        (key, movieEvent, intermediateValue) -> Balance(movieEvent, intermediateValue), // movieEvent is nothing but value, at t=0, intermediateValue = 0, but will be updated accordingly.
                                                                // if (k,v,s) -> {v+s} then initially s=0, if v=1  for k=1 then s=v+s => 1 until it reaches final value for a particular key
                        jsonSerde,  // returned value serde
                        "agg"     // State store - Just like State store in spark. stores the state/intermediate result of the aggregation. If the next input for a particular key arrives after initial batch was ended, it uses the previous state/value from the state store and update/utilize it.
                );
        dataProcessor.to(Serdes.String(), jsonSerde,"output_topic"); // .to() writes the KTable output to new Kafka Stream -> output_topic.
        KafkaStreams streams = new KafkaStreams(myStreamBuilder, streamConfiguration); // create kafka stream object
        streams.cleanUp();
        streams.start();        // start the streams application
        System.out.println(streams.toString()); // for debugging purposes, shows us the streams metrics
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));   //Allows jvm to shutdown gracefully incase of interruptions/errors. hook which makes the background thread to close while jvm shuts down. it is a basic operation that belongs to java and not of streams.
    }
}
