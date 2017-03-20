package com.barclays.streaming.processor;

import java.io.Serializable;

public interface StreamProcessor<I,O> extends Serializable {
	
	//Consumer getInputConsumer(StreamConsumerConfig config);

	O process(I input);
	
	
}
