package com.spark.java.sparkStreaming;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class WindowBatchInterval {
	
	 public static void main(String[] args) {
	    	//Window Specific property if Hadoop is not instaalled or HADOOP_HOME is not set
			 System.setProperty("hadoop.home.dir", "C:\\Users\\sk250102\\Downloads\\bigdataSetup\\hadoop");
	    	//Logger rootLogger = LogManager.getRootLogger();
	   		//rootLogger.setLevel(Level.WARN); 
	        SparkConf conf = new SparkConf().setAppName("KafkaExample").setMaster("local[2]");
	        
	     
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        JavaStreamingContext streamingContext = new JavaStreamingContext(sc, Durations.seconds(10));
	        streamingContext.checkpoint("C:\\Users\\sk250102\\Downloads\\bigdataSetup\\hadoop\\checkpoint");
	        Logger rootLogger = LogManager.getRootLogger();
	   		rootLogger.setLevel(Level.WARN); 
	   		
		    JavaReceiverInputDStream<String> StreamingLines = streamingContext.socketTextStream( "10.0.75.1", Integer.parseInt("9000"), StorageLevels.MEMORY_AND_DISK_SER);
		    
		    JavaDStream<String> words = StreamingLines.flatMap( str -> Arrays.asList(str.split(" ")).iterator() );
		   
		    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(str-> new Tuple2<>(str, 1)).reduceByKey((count1,count2) ->count1+count2 );
		   
		    //wordCounts.print();
		   // wordCounts.window(Durations.seconds(25)).countByValue()
	      // .foreachRDD(tRDD -> tRDD.foreach(x->System.out.println(new Date()+" ::The window count tag is ::"+x._1() +" and the val is ::"+x._2())));
		    
		    
		       JavaDStream<Long> tem = wordCounts.countByWindow(Durations.seconds(30),Durations.seconds(12));
		       tem.print();
		       
		    /*   tem.foreachRDD(new VoidFunction<JavaRDD<Long>>() {
				
				@Override
				public void call(JavaRDD<Long> arg0) throws Exception {
					arg0.foreach(new VoidFunction<Long>() {

						@Override
						public void call(Long arg0) throws Exception {
							System.out.println("the value is ::"+arg0);
							
						}
					});
					//System.out.println("the value is ::"+arg0.count());
					
					
				}
			});
		    		*/
		    		//.foreachRDD(tRDD -> tRDD.foreach(x->System.out.println(new Date()+" ::The window count tag is ::"+x._1()._1() +" and the val is ::"+x._1()._2())));
		  
		    
		    
		    
		    // wordCounts.window(Durations.seconds(12),Durations.seconds(8)).countByValue()
	       //.foreachRDD(tRDD -> tRDD.foreach(x->System.out.println(new Date()+" ::The window count tag is ::"+x._1() +" and the val is ::"+x._2())));
		   // wordCounts.window(Durations.seconds(2),Durations.seconds(2)).countByValue()
	      // .foreachRDD(tRDD -> tRDD.foreach(x->System.out.println(new Date()+" ::The window count tag is ::"+x._1() +" and the val is ::"+x._2())));
		   // wordCounts.window(Durations.seconds(12),Durations.seconds(12)).countByValue()
	       //.foreachRDD(tRDD -> tRDD.foreach(x->System.out.println(new Date()+" ::The window count tag is ::"+x._1() +" and the val is ::"+x._2())));
	       
		    //comment these two operation to make it run
		    //wordCounts.window(Durations.minutes(5),Durations.minutes(2)).countByValue()
	       //.foreachRDD(tRDD -> tRDD.foreach(x->System.out.println(new Date()+" ::The window count tag is ::"+x._1() +" and the val is ::"+x._2())));
		   // wordCounts.window(Durations.minutes(10),Durations.minutes(1)).countByValue()
	      // .foreachRDD(tRDD -> tRDD.foreach(x->System.out.println(new Date()+" ::The window count tag is ::"+x._1() +" and the val is ::"+x._2())));
	       
	        streamingContext.start();
	        try {
				streamingContext.awaitTermination();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	 }

}
