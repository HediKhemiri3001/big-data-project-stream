package projet.stream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import com.google.gson.Gson;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json4s.jackson.Json;

public class KafkaStreamProducer {
    public static int count = 1;
    public static void main(String[] args) throws Exception {

        String topicName = "tweets-topic";
        String fileName = args[0];

        // Set up Kafka properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop-master:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Read CSV file and send each row as a message to Kafka
        Reader reader = new FileReader(args[0]);
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader());

        for (CSVRecord csvRecord : csvParser) {
            int id = count +1;
            count = count +1;
            String username = csvRecord.get("user_name");
            String content = csvRecord.get("text");
            String date = csvRecord.get("date");
            String location = csvRecord.get("user_location");
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date dateObj = dateFormat.parse(date);
            // Need to do preprocessing on country column to get exact country name.
            Tweet tweet = new Tweet(id, username,dateObj,content,location);
            Gson gson = new Gson();
            String JsonTweet = gson.toJson(tweet);
            System.out.println("Sending "+ JsonTweet+".");
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, JsonTweet);
            producer.send(record);
            try {
                Thread.sleep(10000); // Sleep for 10 seconds
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
        }
        reader.close();

        // Close Kafka producer
        producer.close();
    }
}


