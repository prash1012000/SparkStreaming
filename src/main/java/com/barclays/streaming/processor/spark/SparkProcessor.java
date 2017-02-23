package com.barclays.streaming.processor.spark;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.barclays.streaming.processor.StreamProcessor;
import com.barclays.streaming.source.Consumer;
import com.barclays.streaming.source.kafka.KafkaSparkConsumer;

public abstract class SparkProcessor<K,V> implements StreamProcessor {

	protected JavaStreamingContext getSparkStreamingContext(String appname, Map sparkParams) {
		SparkConf sparkConf = new SparkConf().setAppName(appname);
		sparkConf.setMaster((String)sparkParams.get("master"));
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		long batchDuration=Long.parseLong((String)sparkParams.get("BatchDuration"));
		JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(batchDuration));
	    return jssc;	
	}

	protected void start(JavaStreamingContext jssc) {
		jssc.start();
	}
	
	protected void waitForTermination(JavaStreamingContext jssc) {
		jssc.awaitTermination();
	}
	
	public void process(String appname, Map sparkParams) {
		JavaStreamingContext jssc = getSparkStreamingContext(appname, sparkParams);
		processSourceStream(sparkParams,jssc);
		start(jssc);
		waitForTermination(jssc);
	}

	protected abstract void processSourceStream(Map params,JavaStreamingContext jssc);
	
}
