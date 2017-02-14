package com.barclays.streaming.source;

import java.util.Map;

public abstract class ConsumerConfig<K, V, KD, VD> {

	protected Class<K> keyClass;
	protected Class<V> valueClass;
	protected Class<KD> keyDecoderClass;
	protected Class<VD> valueDecoderClass;
	protected Map<String,String> params;

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
	public Map<String, String> getParams() {
		return params;
	}
	public void setParams(Map<String, String> params) {
		this.params =params;
	}

}