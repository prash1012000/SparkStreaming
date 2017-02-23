package com.barclays.streaming.processor;

import java.util.Map;

import com.barclays.streaming.source.Consumer;
import com.barclays.streaming.source.StreamConsumerConfig;

public interface StreamProcessor {
	
	Consumer getInputConsumer(StreamConsumerConfig config);

	void process(String appname, Map sparkParams);
	
	//void setProcessor(IProcessor process);
	
	
}
