package com.barclays.streaming.source;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class StreamConsumerConfig {

	protected Map sourceParams=new HashMap();

	public Map<String, String> getParams() {
		return sourceParams;
	}
	public void setParams(Map<String, String> params) {
		this.sourceParams =params;
	}
	
	public <T> void addPropertyIfNotNull(String key, T value) {
        if (value != null) {
            // Kafka expects all properties as String
        	sourceParams.put(key, value.toString());
        }
    }


}