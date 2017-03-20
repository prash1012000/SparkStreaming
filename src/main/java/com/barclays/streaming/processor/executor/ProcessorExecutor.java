package com.barclays.streaming.processor.executor;

import com.barclays.streaming.processor.StreamProcessor;

public interface ProcessorExecutor<I,P> {
	
	public void execute(I input,P processor);

}
