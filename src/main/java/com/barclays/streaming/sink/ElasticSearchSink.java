package com.barclays.streaming.sink;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

public class ElasticSearchSink implements StreamSink<JavaDStreamLike<?,?,?>> {

	@Override
	public void sink(JavaDStreamLike<?,?,?> input) {
		//input.foreachRDD(rdd->{JavaEsSpark.saveToEs((JavaRDD<?>) rdd,"sparkdata/counter");});
		
		
		
	}

	
	

}
