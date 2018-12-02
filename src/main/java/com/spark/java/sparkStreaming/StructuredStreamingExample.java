package com.spark.java.sparkStreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import com.fasterxml.jackson.databind.ObjectMapper;

public class StructuredStreamingExample {

	public static void main(String[] args) throws StreamingQueryException {
		//System.setProperty("hadoop.home.dir", "C:\\softwares\\Winutils");
		SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("structured Streaming Example")
				.getOrCreate();

		Dataset<Row> inStream = sparkSession.readStream().format("socket").option("host", "10.0.75.1")
				.option("port", 9000).load();

		Dataset<FlightDetails> dsFlightDetails = inStream.as(Encoders.STRING()).map(x -> {
			ObjectMapper mapper = new ObjectMapper();
			return mapper.readValue(x, FlightDetails.class);

		}, Encoders.bean(FlightDetails.class));
		
		
		dsFlightDetails.createOrReplaceTempView("flight_details");
		
		Dataset<Row> avdFlightDetails = sparkSession.sql("select flightId, avg(temperature) from flight_details group by flightId");
		
		StreamingQuery query = avdFlightDetails.writeStream()
				  .outputMode("complete")
				  .format("console")
				  .start();

				query.awaitTermination();
		

	}

}
