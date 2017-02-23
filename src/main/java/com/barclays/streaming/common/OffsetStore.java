package com.barclays.streaming.common;

import kafka.common.TopicAndPartition;

import java.util.Map;

import org.apache.spark.rdd.RDD;

public interface OffsetStore {
	
	Map<TopicAndPartition, Long> readOffsets(String topic);

	public <T> void saveOffsets(String topic, RDD<T> rdd);

}
