package com.str.consumer;

import java.io.IOException;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.str.domain.Model;

@Component
public class ReplyingConsumer {
	@KafkaListener(topics = "${kafka.topic.request-topic}")
	@SendTo
	public String listen(String input) throws InterruptedException, JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		Model request = mapper.readValue(input, Model.class);
		int sum = request.getFirstNumber() + request.getSecondNumber();
		request.setAdditionalProperty("sum", sum);
		return mapper.writeValueAsString(request);
	}
}
