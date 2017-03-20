package com.barclays.streaming.sink;

public interface StreamSink<I> {
	
	public void sink(I input);

}
