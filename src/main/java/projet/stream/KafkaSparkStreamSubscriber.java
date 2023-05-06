package projet.stream;

import com.google.gson.Gson;
import com.mongodb.*;
import com.mongodb.client.MongoClients;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.bson.Document;

import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class KafkaSparkStreamSubscriber {
    public static void main(String[] args) throws InterruptedException {
        // Set up Spark Streaming context
        SparkConf conf = new SparkConf().setAppName("KafkaSparkMongoDB").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // Set up Kafka parameters
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "hadoop-master:9092");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "mongodb-persister");

        // Create Kafka stream
        Collection<String> topics = Arrays.asList("tweets-topic");
        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        JavaPairDStream<String, String> kafkaRecords = kafkaStream
                .mapToPair(record -> new Tuple2<>(record.key(), record.value()));

        // Write output to MongoDB
        kafkaRecords.foreachRDD(rdd -> {
            rdd.foreachPartition(records -> {
                ServerAddress serverAddress = new ServerAddress("mongodb", 27017);
                MongoClientSettings settings = MongoClientSettings.builder()
                        .applyToClusterSettings(builder -> builder.hosts(Collections.singletonList(serverAddress)))
                        .build();
                MongoClient mongoClient = MongoClients.create(settings);
                MongoDatabase database = mongoClient.getDatabase("projet-bigdata");
                MongoCollection<Document> collection = database.getCollection("pandemic_tweets");

                records.forEachRemaining(record -> {
                    String tweetAsAString = record._2();
                    Gson gson = new Gson();
                    TweetLocationToCountryHelper helper = new TweetLocationToCountryHelper(args[0]);
                    Tweet tweet = gson.fromJson(tweetAsAString, Tweet.class);
                    try {
                        String country = helper.getCountry(tweet.getLocation());
                        Document document = new Document("id", tweet.getId()).append("username",tweet.getUsername()).append("date",tweet.getDate()).append("content", tweet.getContent()).append("location", tweet.getLocation()).append("country", country);
                        collection.insertOne(document);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                });
                mongoClient.close();

            });
        });

        // Start Spark Streaming context
        jssc.start();
        jssc.awaitTermination();
    }
}
