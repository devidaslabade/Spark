package com.spark.java.sparkStreaming;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import scala.Tuple2;

public class KafkaOffsetExample  {

    public static void main(String[] args) {
    	//Window Specific property if Hadoop is not instaalled or HADOOP_HOME is not set
		 System.setProperty("hadoop.home.dir", "C:\\Users\\sk250102\\Downloads\\bigdataSetup\\hadoop");
    	//Logger rootLogger = LogManager.getRootLogger();
   		//rootLogger.setLevel(Level.WARN); 
        SparkConf conf = new SparkConf().setAppName("KafkaExample").setMaster("local[*]");    
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext streamingContext = new JavaStreamingContext(sc, Durations.seconds(10));
        streamingContext.checkpoint("C:\\Users\\sk250102\\Downloads\\bigdataSetup\\hadoop\\checkpoint");
        Logger rootLogger = LogManager.getRootLogger();
   		rootLogger.setLevel(Level.WARN); 
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.0.75.1:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "gp_m_1");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("test");

        final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext,LocationStrategies.PreferConsistent(),
        				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        
        stream.foreachRDD( dstreamRDD -> {
        	OffsetRange[] offsetRanges = ((HasOffsetRanges) dstreamRDD.rdd()).offsetRanges();
        	for( OffsetRange or: offsetRanges ) {
        		System.out.println("the offset topic is: "+or.topic()+" :: parition "+or.partition()+" :: from offset ::"+or.fromOffset()+
        		" :: untill offset :: "+or.untilOffset()+" :: the count is :: "+or.count()+" :: the hashcode is :: "+or.hashCode());
        	}
        	
        	dstreamRDD.foreach(kafkaMsg -> {
        		System.out.println("The key is :: "+kafkaMsg.key());
        		System.out.println("The value is ::"+ kafkaMsg.value());
        	});
        	
        	((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);

        });
        

        streamingContext.start();
        try {
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

	
}
