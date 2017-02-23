package com.barclays.streaming.processor.spark.kafka;

import java.util.Map;

import org.apache.hadoop.fs.FileUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.barclays.streaming.common.ZookeeperOffsetStore;
import com.barclays.streaming.processor.spark.SparkProcessor;
import com.barclays.streaming.source.Consumer;
import com.barclays.streaming.source.StreamConsumerConfig;
import com.barclays.streaming.source.kafka.KafkaSparkConsumer;
import com.barclays.streaming.source.kafka.KafkaSparkConsumerConfig;

public class KafkaSparkStreamProcessor<K, V, T> extends SparkProcessor {

	@Override
	public Consumer getInputConsumer(StreamConsumerConfig config) {
		return new KafkaSparkConsumer(config);
	}
	
	@SuppressWarnings("unchecked")
	protected void processSourceStream(Map params,JavaStreamingContext jssc){
		Consumer consumer =  getInputConsumer((KafkaSparkConsumerConfig)params.get("sparkConfig"));
		JavaDStreamLike<?, ?, ?> messages=(JavaDStreamLike<?, ?, ?>)consumer.getDataStream(jssc);
		//messages.saveAsNewAPIHadoopFiles("hdfs://127.0.0.1:50071/ws/output", "txt");
		JavaDStream<String> lines = messages.map(tuple -> {
			String value = tuple.toString();
			System.out.println("Value is "+value);
			return value;
		});
		// opFilepath:"file:///c:/output/kafkalines";
		messages.dstream().saveAsTextFiles("hdfs://127.0.0.1:50071/ws/output", "txt");
		
		
	}

}
