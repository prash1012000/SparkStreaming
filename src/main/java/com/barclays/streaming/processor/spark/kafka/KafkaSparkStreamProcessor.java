package com.barclays.streaming.processor.spark.kafka;

import java.util.Map;

import org.apache.hadoop.fs.FileUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.barclays.streaming.processor.spark.SparkProcessor;
import com.barclays.streaming.source.Consumer;
import com.barclays.streaming.source.kafka.KafkaSparkConsumer;

public class KafkaSparkStreamProcessor<K, V> extends SparkProcessor {

	@Override
	public Consumer getInputConsumer(Object config) {
		return new KafkaSparkConsumer(config);
	}
	
	protected void processSourceStream(Consumer consumer,JavaStreamingContext jssc){
		JavaPairInputDStream<K, V> messages=(JavaPairInputDStream<K, V>) consumer.getDataStream(jssc);
		JavaDStream<String> lines = messages.map(tuple -> {
			/*String key=tuple._1.toString();
			System.out.println("Key is "+key);*/
			String value = tuple._2.toString();
			System.out.println("Value is "+value);
			return value;
		});
		// opFilepath:"file:///c:/output/kafkalines" );
		lines.dstream().saveAsTextFiles("file:///c:/output/kafkalines", "txt");
	}

}
