package com.barclays.streaming.processor;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;


public class SparkProcessor<T, R> implements StreamProcessor<JavaDStream<T>,JavaDStream<String>> {

	public JavaDStream<String> process(JavaDStream<T> dstream) {
		Map<String,String> values=new HashMap<String, String>();
		JavaDStream<Long> messageCount=dstream.count();
		JavaDStream<Map<String, String>> r=messageCount.map( value->{values.put("msgcount",value.toString());
								 values.put("appname","sparkstreaming");
								 values.put("generator","processor");
								 return values;});
		r.foreachRDD(r1 -> {
			JavaEsSpark.saveToEs(r1,"sparkdata/counter");
		});
		
		
		return null;
	
	}

	
}
