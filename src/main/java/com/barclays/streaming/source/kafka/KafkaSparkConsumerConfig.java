package com.barclays.streaming.source.kafka;

import java.util.Set;

import com.barclays.streaming.source.StreamConsumerConfig;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import kafka.serializer.Decoder;

public class KafkaSparkConsumerConfig<K, V, KD extends Decoder<K>, VD extends Decoder<V>, R>
		extends StreamConsumerConfig {
	
	
	private Class<K> keyClass;
	
	private Class<V> valueClass;
	
	private Class<KD> keyDecoderClass;
	
	private Class<VD> valueDecoderClass;
	
	private Class<R> recordDecoderClass;

	private Set<String> topics;

	private String groupId;
	
	private String brokers;
	
	private String clientId;

	private Integer fetchMinBytes = 1024;
	
	private Integer heartbeatIntervalMs = 3000;

	private String autoOffsetReset = "largest";

	private Integer autoCommitIntervalMs = 5000;

	private Integer fetchWaitMaxMs = 500;

	private String consumerId;

	private Boolean autoCommitEnable = true;


	private Integer reconnectBackoffMs = 50;

	public Set<String> getTopics() {
		return topics;
	}

	public void setTopics(Set<String> topics) {
		this.topics = topics;
	}

	
	public Integer getFetchMinBytes() {
		return fetchMinBytes;
	}

	/**
	 * The minimum amount of data the server should return for a fetch request.
	 * If insufficient data is available the request will wait for that much
	 * data to accumulate before answering the request.
	 */
	public void setFetchMinBytes(Integer fetchMinBytes) {
		this.fetchMinBytes = fetchMinBytes;
	}

	public String getAutoOffsetReset() {
		return autoOffsetReset;
	}

	/**
	 * What to do when there is no initial offset in ZooKeeper or if an offset
	 * is out of range: smallest : automatically reset the offset to the
	 * smallest offset largest : automatically reset the offset to the largest
	 * offset fail: throw exception to the consumer
	 */
	public void setAutoOffsetReset(String autoOffsetReset) {
		this.autoOffsetReset = autoOffsetReset;
	}

	public String getConsumerId() {
		return consumerId;
	}

	/**
	 * Generated automatically if not set.
	 */
	public void setConsumerId(String consumerId) {
		this.consumerId = consumerId;
	}

	public Boolean isAutoCommitEnable() {
		return autoCommitEnable;
	}

	/**
	 * If true, periodically commit to ZooKeeper the offset of messages already
	 * fetched by the consumer. This committed offset will be used when the
	 * process fails as the position from which the new consumer will begin.
	 */
	public void setAutoCommitEnable(Boolean autoCommitEnable) {
		this.autoCommitEnable = autoCommitEnable;
	}

	public Integer getAutoCommitIntervalMs() {
		return autoCommitIntervalMs;
	}

	/**
	 * The frequency in ms that the consumer offsets are committed to zookeeper.
	 */
	public void setAutoCommitIntervalMs(Integer autoCommitIntervalMs) {
		this.autoCommitIntervalMs = autoCommitIntervalMs;
	}

	public Integer getFetchWaitMaxMs() {
		return fetchWaitMaxMs;
	}

	/**
	 * The maximum amount of time the server will block before answering the
	 * fetch request if there isn't sufficient data to immediately satisfy
	 * fetch.min.bytes
	 */
	public void setFetchWaitMaxMs(Integer fetchWaitMaxMs) {
		this.fetchWaitMaxMs = fetchWaitMaxMs;
	}

	public String getClientId() {
		return clientId;
	}

	/**
	 * The client id is a user-specified string sent in each request to help
	 * trace calls. It should logically identify the application making the
	 * request.
	 */
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public Integer getReconnectBackoffMs() {
		return reconnectBackoffMs;
	}

	/**
	 * The amount of time to wait before attempting to reconnect to a given
	 * host. This avoids repeatedly connecting to a host in a tight loop. This
	 * backoff applies to all requests sent by the consumer to the broker.
	 */
	public void setReconnectBackoffMs(Integer reconnectBackoffMs) {
		this.reconnectBackoffMs = reconnectBackoffMs;
	}

	public String getBrokers() {
		return brokers;
	}

	/**
	 * URL of the Kafka brokers to use. The format is host1:port1,host2:port2,
	 * and the list can be a subset of brokers or a VIP pointing to a subset of
	 * brokers.
	 * <p/>
	 * This option is known as <tt>bootstrap.servers</tt> in the Kafka
	 * documentation.
	 */
	public void setBrokers(String brokers) {
		this.brokers = brokers;
	}
	
	public Class<K> getKeyClass() {
		return keyClass;
	}
	public void setKeyClass(Class<K> keyClass) {
		this.keyClass = keyClass;
	}
	public Class<V> getValueClass() {
		return valueClass;
	}
	public void setValueClass(Class<V> valueClass) {
		this.valueClass = valueClass;
	}
	public Class<KD> getKeyDecoderClass() {
		return keyDecoderClass;
	}
	public void setKeyDecoderClass(Class<KD> keyDecoderClass) {
		this.keyDecoderClass = keyDecoderClass;
	}
	public Class<VD> getValueDecoderClass() {
		return valueDecoderClass;
	}
	public void setValueDecoderClass(Class<VD> valueDecoderClass) {
		this.valueDecoderClass = valueDecoderClass;
	}
	
	public Integer getHeartbeatIntervalMs() {
		return heartbeatIntervalMs;
	}

	public void setHeartbeatIntervalMs(Integer heartbeatIntervalMs) {
		this.heartbeatIntervalMs = heartbeatIntervalMs;
	}

	public Class<R> getRecordDecoderClass() {
		return recordDecoderClass;
	}

	public void setRecordDecoderClass(Class<R> recordDecoderClass) {
		this.recordDecoderClass = recordDecoderClass;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}
	
	public void createConsumerProperties() {
		addPropertyIfNotNull(KafkaConstants.METADATA_BROKER_LIST, getBrokers());
		addPropertyIfNotNull(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,getAutoCommitIntervalMs());
		addPropertyIfNotNull(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, getAutoOffsetReset());
		addPropertyIfNotNull(ConsumerConfig.CLIENT_ID_CONFIG, getClientId());
		addPropertyIfNotNull(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, isAutoCommitEnable());
		addPropertyIfNotNull(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, getFetchWaitMaxMs());
		addPropertyIfNotNull(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, getFetchMinBytes());
		addPropertyIfNotNull(ConsumerConfig.GROUP_ID_CONFIG,getGroupId());		
		addPropertyIfNotNull(ConsumerConfig.HEARTBEAT_FREQUENCY,getHeartbeatIntervalMs());
		addPropertyIfNotNull(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, getReconnectBackoffMs());
	}
}
