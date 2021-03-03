*************************************************************************
*				README					*
*************************************************************************
1)Download the zip file to your computer from https://github.com/deepakrajkanchipuramswami/KafkaJson.git or git clone https://github.com/deepakrajkanchipuramswami/KafkaJson.git using CLI.

2)If you already have kafka, run the below commands else refer to the kafka Installation section before proceeding to the next step.

3)Start a Command line and navigate to the folder where you have installed Kafka.(Ideally C:/kafka_2.12-2.0.0)

4)Run the command - zookeeper-server-start.bat config\zookeeper.properties

5)Repeat step 3 and open a new terminal

6)Run the command - kafka-server-start.bat config\server.properties

7)Repeat step 3 and open a new terminal

8)Run the below command to create a topic named input_topic
kafka-topics.bat --zookeeper localhost:2181 --create --topic input_topic --partitions 3 --replication-factor 1

9)Repeat step 3 and open a new terminal

10)Run the below command to create a topic named output_topic
kafka-topics.bat --zookeeper localhost:2181 --create --topic output_topic --partitions 1 --replication-factor 1 --config cleanup.policy=compact

11)Repeat step 3 and open a new terminal

12)Navigate to the Producer folder where you performed git clone and execute the command

java -jar New-1.0-SNAPSHOT-jar-with-dependencies.jar

13)Navigate to the Streams folder where you performed git clone and execute the command

java -jar New-1.0-SNAPSHOT-jar-with-dependencies.jar

(Optional) Run the below command in a new command to identify whether the data is being generated by Producer and sent to Kafka
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic input_topic

14) Run the below command in a new command to identify whether the kafka Streams application is running successfully

kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic output_topic --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.value=true  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

****************************************************************************
*			TO PERFORM UNIT TESTING				   *
****************************************************************************
1) Perform all the steps in the above code except step 12.

2)Instead of performing step 12 in the above section, perform the below step

Navigate to the Unit testing folder where you performed git clone and execute the command

java -jar New-1.0-SNAPSHOT-jar-with-dependencies.jar

3)Check the streams output, you will find that the value of firstSeen for a particular UserId remains the same for all record whereas the lastSeen will vary.

This shows that a particular user has seen his/her first movie at firstSeen time and last saw a movie at lastSeen.

****************************************************************************
*			KAFKA INSTALLATION(OPTIONAL)			   *
****************************************************************************
The below steps for the Kafka-Installation steps are acquired from DataCumulus resources section.

1.Download and Setup Java 8 JDK

2.Download the Kafka binaries from https://kafka.apache.org/downloads

3.Extract Kafka at the root of C:\

4.Setup Kafka bins in the Environment variables section by editing Path

5.Try Kafka commands using kafka-topics.bat (for example)

6.Edit Zookeeper & Kafka configs using NotePad++ https://notepad-plus-plus.org/download/

7.zookeeper.properties: dataDir=C:/kafka_2.12-2.0.0/data/zookeeper (yes the slashes are inversed)

8.server.properties: log.dirs=C:/kafka_2.12-2.0.0/data/kafka (yes the slashes are inversed)

