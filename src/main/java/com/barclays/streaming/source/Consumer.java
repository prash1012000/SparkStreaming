package com.barclays.streaming.source;

import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public interface Consumer<K, V, KD, VD> {
	public JavaDStreamLike<?, ?, ?> getDataStream(JavaStreamingContext jssc);

}
