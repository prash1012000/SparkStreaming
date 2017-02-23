package com.barclays.streaming.common;

import com.barclays.streaming.common.Stopwatch;
import kafka.common.TopicAndPartition;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;
import scala.Option;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperOffsetStore implements OffsetStore {

	private ZkClient zkClient = null;
	private String zkPath = null;
	public static final Logger logger = LoggerFactory.getLogger(ZookeeperOffsetStore.class);

	public ZookeeperOffsetStore(String zkHosts, String zkPath,int sessionTimeout,int connectionTimeout) {
		this.zkPath=zkPath;
		zkClient = new ZkClient(zkHosts, sessionTimeout, 10000);
	}

	public Map<TopicAndPartition, Long> readOffsets(String topic) {

		logger.info("Reading offsets from ZooKeeper");
		Map<TopicAndPartition, Long> offsets = new HashMap<>();
		Stopwatch stopwatch = new Stopwatch();
		Option<String> offsetsRangesStrOpt = ZkUtils.readDataMaybeNull(zkClient, zkPath)._1();
		String offsetsRangesStr = offsetsRangesStrOpt.isDefined() ? offsetsRangesStrOpt.get() : null;
		if (offsetsRangesStr != null) {
			String[] offsetArray = offsetsRangesStr.split(",");
			for (String offsetValue : offsetArray) {
				String[] values = offsetValue.split(":");
				TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
						Integer.parseInt(values[0].toString()));
				offsets.put(topicAndPartition, Long.parseLong(values[1]));
			}

		}
		return offsets;

	}

	public <T> void saveOffsets(String topic, RDD<T> rdd) {

		logger.info("Saving offsets to ZooKeeper");
		Stopwatch stopwatch = new Stopwatch();

		OffsetRange[] offsetsRanges = ((HasOffsetRanges) rdd).offsetRanges();
		StringBuffer offsetsRangesStr = new StringBuffer();
		for (OffsetRange range : offsetsRanges) {
			if (offsetsRangesStr.length() > 0) {
				offsetsRangesStr.append(",");
			}
			offsetsRangesStr.append(range.partition() + ":" + range.fromOffset());
		}
		logger.debug("Writing offsets to ZooKeeper " + offsetsRangesStr);
		ZkUtils.updatePersistentPath(zkClient, zkPath, offsetsRangesStr.toString());

		logger.info("Done updating offsets in ZooKeeper. Took " + stopwatch);

	}

}
