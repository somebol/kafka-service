package org.springframework.kafka.requestreply;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;

public class JSONConverter implements Deserializer<KMessage<Map<String, String>>>, Serializer<KMessage<Map<String, String>>> {
	
	private final Gson gson = new Gson();
	
	@Override
	public byte[] serialize(String topic, KMessage<Map<String, String>> data) {
		return gson.toJson(data).getBytes();
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public KMessage<Map<String, String>> deserialize(String topic, byte[] data) {
		return gson.fromJson(new String(data), KMessage.class);
	}

	@Override
	public void close() {
		
	}

}
