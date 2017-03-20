package com.barclays.streaming.source.kafka;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.util.Time;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.barclays.streaming.source.Consumer;
import com.barclays.streaming.source.StreamConsumerConfig;

import scala.Tuple2;

public class KafkaSource<T, R> implements Serializable {
	private static JavaStreamingContext jssc = null;

	public void createSparkStreamingContext(String appname, Map sparkParams) {
		if (jssc == null) {
			SparkConf sparkConf = new SparkConf().setAppName(appname);
			sparkConf.setMaster((String) sparkParams.get("master"));
			JavaSparkContext sc = new JavaSparkContext(sparkConf);
			long batchDuration = Long.parseLong((String) sparkParams.get("BatchDuration"));
			jssc = new JavaStreamingContext(sc, Durations.seconds(batchDuration));
		}

	}

	public void start() {
		if (jssc != null)
			jssc.start();
	}

	public void waitForTermination() {
		if (jssc != null)
			jssc.awaitTermination();
	}

	public JavaInputDStream<T> getKafkaData(String appname, Map params) {

		createSparkStreamingContext(appname, params);
		KafkaSparkConsumerConfig config = (KafkaSparkConsumerConfig) params.get("sparkConfig");
		Consumer consumer = getInputConsumer(config);
		JavaInputDStream<T> messages = (JavaInputDStream<T>) consumer.getDataStream(jssc);
		messages.persist();
		Map<String, String> values = new HashMap<String, String>();
		JavaDStream<Long> messageCount = messages.count();
		JavaDStream<Map<String, String>> r = messageCount.map(value -> {
			values.put("msgcount", value.toString());
			values.put("appname", appname);
			values.put("generator", "source");
			return values;
		});
		/*
		 * r.foreachRDD(r1 -> { JavaEsSpark.saveToEs(r1,"sparkdata/counter");
		 * });
		 */
		r.foreachRDD(r1 -> {

			r1.foreachPartition(itr -> {
				KafkaWriter writer = getKafkaWriter();
				while (itr.hasNext()) {
					Map<String, String> map = itr.next();
					if(Long.valueOf(map.get("msgcount"))>0){
						ObjectMapper mapperObj = new ObjectMapper();
						String jsonResp = null;
						try {
							jsonResp = mapperObj.writeValueAsString(map);
							System.out.println("JSON Response is" + jsonResp);
						} catch (IOException e) {
							e.printStackTrace();
						}
						writer.publishMesssage("monitor", null, jsonResp);
					}	
				}
			});

		});
		return messages;

	}

	public KafkaWriter getKafkaWriter() {
		KafkaWriter writer = new KafkaWriter();
		Properties producerProps = new Properties();
		producerProps.put("metadata.broker.list", "localhost:9092");
		producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
		//producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		//producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProps.put("request.required.acks", "1");
		writer.initialize(producerProps);
		return writer;

	}

	public Consumer getInputConsumer(StreamConsumerConfig config) {
		return new KafkaSparkConsumer(config);
	}

}
