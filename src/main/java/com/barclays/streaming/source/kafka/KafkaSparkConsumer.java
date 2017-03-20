package com.barclays.streaming.source.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.barclays.streaming.common.ZookeeperOffsetStore;
import com.barclays.streaming.source.Consumer;
import com.barclays.streaming.source.StreamConsumerConfig;

import kafka.common.TopicAndPartition;
import kafka.serializer.Decoder;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class KafkaSparkConsumer<K extends Object, V extends Object, KD extends Decoder<K>, VD extends Decoder<V>, R>
		implements Consumer {

	private KafkaSparkConsumerConfig<K, V, Decoder<K>, Decoder<V>, R> config;

	public KafkaSparkConsumer(StreamConsumerConfig config2) {
		this.config = (KafkaSparkConsumerConfig) config2;
		config.createConsumerProperties();
	}

	public KafkaSparkConsumerConfig<K, V, Decoder<K>, Decoder<V>, R> getConfig() {
		return config;
	}

	@SuppressWarnings("unchecked")
	@Override
	public JavaDStreamLike<?, ?, ?> getDataStream(JavaStreamingContext jssc) {
		JavaDStreamLike<?, ?, ?> kafkaStream = null;
		ZookeeperOffsetStore store = new ZookeeperOffsetStore("localhost:2181", "/offset/kafka", 100000, 100000);
		String firstTopic = ((KafkaSparkConsumerConfig<K, V, KD, VD, R>) config).getFirstTopic();
		Map<TopicAndPartition, Long> partitionOffsetMap = store.readOffsets(firstTopic);
		if (partitionOffsetMap.size() == 0) {
			partitionOffsetMap = getOffsets("localhost:2181", config.getGroupId(), "streaming");
			fillInLatestOffsets(partitionOffsetMap, config.getParams());
		}

		kafkaStream = KafkaUtils.createDirectStream(jssc, config.getKeyClass(), config.getValueClass(),
				config.getKeyDecoderClass(), config.getValueDecoderClass(), Tuple2.class, config.getParams(),
				partitionOffsetMap, mmd -> {
					if (mmd.key() != null){
						System.out.println("Key is " + mmd.key().toString());
					}	
					return (Tuple2) new Tuple2(mmd.key(), mmd.message());
				});
		
		kafkaStream.foreachRDD(rdd -> {
			if (rdd != null && rdd.rdd() != null)
				store.saveOffsets(firstTopic, rdd.rdd());
		});

		return kafkaStream;

	}

	public static void fillInLatestOffsets(Map<TopicAndPartition, Long> offsets, Map kafkaParams) {

		Set needOffset = new HashSet<>();
		for (Map.Entry<TopicAndPartition, Long> entry : offsets.entrySet()) {
			if (entry.getValue() == null) {
				needOffset.add(entry.getKey());
			}
		}
		@SuppressWarnings("unchecked")
		scala.collection.immutable.Map<String, String> kafkaParamsScalaMap = (scala.collection.immutable.Map<String, String>) scala.collection.immutable.Map$.MODULE$
				.apply(JavaConversions.mapAsScalaMap(kafkaParams).toSeq());
		@SuppressWarnings("unchecked")
		scala.collection.immutable.Set<TopicAndPartition> needOffsetScalaSet = (scala.collection.immutable.Set<TopicAndPartition>) scala.collection.immutable.Set$.MODULE$
				.apply(JavaConversions.asScalaSet(needOffset).toSeq());

		KafkaCluster kc = new KafkaCluster(kafkaParamsScalaMap);
		Map<TopicAndPartition, ?> leaderOffsets = JavaConversions
				.mapAsJavaMap(kc.getLatestLeaderOffsets(needOffsetScalaSet).right().get());
		for (Map.Entry<TopicAndPartition, ?> entry : leaderOffsets.entrySet()) {
			TopicAndPartition tAndP = entry.getKey();
			Long leaderOffset = ((LeaderOffset) entry.getValue()).offset();
			offsets.put(tAndP, leaderOffset);
		}

	}

	public static <A> Map<TopicAndPartition, Long> getOffsets(String zkServers, String groupID, String topic) {
		ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupID, topic);
		Map<TopicAndPartition, Long> offsets = new HashMap<>();
		ZkClient zkClient = new ZkClient(zkServers);
		zkClient.setZkSerializer(new ZkSerializer() {
			@Override
			public byte[] serialize(Object o) throws ZkMarshallingError {
				return ZKStringSerializer.serialize(o);
			}

			@Override
			public Object deserialize(byte[] bytes) throws ZkMarshallingError {
				return ZKStringSerializer.deserialize(bytes);
			}
		});
		@SuppressWarnings("unchecked")
		scala.collection.mutable.Map<String, Seq<Object>> partitionsmap = ZkUtils.getPartitionsForTopics(zkClient,
				JavaConversions.asScalaBuffer(Collections.singletonList(topic)));
		Map<String, Seq<Object>> javaPartitions = JavaConversions.mapAsJavaMap(partitionsmap);
		List<Object> partitions = null;
		// while(javaPartitions.values().iterator().hasNext()){
		partitions = JavaConversions.seqAsJavaList(javaPartitions.values().iterator().next());
		// }
		for (Object partition : partitions) {
			String partitionOffsetPath = topicDirs.consumerOffsetDir() + "/" + partition;
			Option<String> maybeOffset = ZkUtils.readDataMaybeNull(zkClient, partitionOffsetPath)._1();
			Long offset = maybeOffset.isDefined() ? Long.parseLong(maybeOffset.get()) : null;
			TopicAndPartition topicAndPartition = new TopicAndPartition(topic, Integer.parseInt(partition.toString()));
			offsets.put(topicAndPartition, offset);
		}

		return offsets;
	}
	
	
	
	

}
