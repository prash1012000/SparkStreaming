package com.barclays.streaming.processor;

import java.util.Map;

import com.barclays.streaming.source.Consumer;
import com.barclays.streaming.source.ConsumerConfig;

public interface StreamProcessor {
	Consumer getInputConsumer(Object object);
	
//	Producer getOutputProducer();

	void process(String appname, Map params);
	
	
}
