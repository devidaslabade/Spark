package com.spark.java.sparkStreaming;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public final class WordCountSocketEx {

	public static void main(String[] args) throws Exception {

		/*
		 * if (args.length < 2) {
		 * System.err.println("Usage: JavaNetworkWordCount <hostname> <port>");
		 * System.exit(1); }
		 */ 
		System.setProperty("hadoop.home.dir", "C:\\Users\\sk250102\\Downloads\\bigdataSetup\\hadoop");

		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("WordCountSocketEx").setMaster("local[2]");		
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));
		
		
		Logger rootLogger = LogManager.getRootLogger();
		rootLogger.setLevel(Level.WARN);

		JavaReceiverInputDStream<String> StreamingLines = streamingContext.socketTextStream("10.0.75.1", Integer.parseInt("9000"),
				StorageLevels.MEMORY_AND_DISK_SER);

		JavaDStream<String> words = StreamingLines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String str) {
				return Arrays.asList(str.split(" ")).iterator();
			}
		});

		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String str) {
				return new Tuple2<>(str, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer count1, Integer count2) {
				return count1 + count2;
			}
		});
		
		
		
         wordCounts.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {
			
			@Override
			public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
				rdd.foreach(new VoidFunction<Tuple2<String,Integer>>() {
					
					@Override
					public void call(Tuple2<String, Integer> arg) throws Exception {
						System.out.println("The count of "+arg._1()+" is "+arg._2());
						
					}
				});
				
			}
		});
		
		//wordCounts.print();
		streamingContext.start();
		streamingContext.awaitTermination();
	}
}