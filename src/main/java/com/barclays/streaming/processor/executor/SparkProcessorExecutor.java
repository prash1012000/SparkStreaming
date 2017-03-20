package com.barclays.streaming.processor.executor;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;

import com.barclays.streaming.processor.SparkProcessor;


public class SparkProcessorExecutor<T, R> implements ProcessorExecutor<JavaInputDStream<T>,SparkProcessor<T, R>> {	

	@Override
	public void execute(JavaInputDStream<T> input, SparkProcessor<T, R> processor) {
		JavaDStream<String> processoutput=processor.process(input);	
		
	}

}
