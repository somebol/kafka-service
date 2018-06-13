package com.str.controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.kafka.requestreply.CorrelationKey;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.str.domain.Model;

@RestController
public class Controller {
	@Autowired
	ReplyingKafkaTemplate<String, String, String> kafkaTemplate;

	@Value("${kafka.topic.request-topic}")
	String requestTopic;

	@Value("${kafka.topic.requestreply-topic}")
	String requestReplyTopic;

	@Value("${kafka.topic.request}")
	String requestTopic2;
	
	@Value("${kafka.topic.reply}")
	String requestReplyTopic2;
	
	@ResponseBody
	@PostMapping(value = "/sum", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	public Model sum(@RequestBody Model request) throws InterruptedException, ExecutionException, JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(requestTopic, mapper.writeValueAsString(request));
		record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, requestReplyTopic.getBytes()));
		RequestReplyFuture<String, String, String> sendAndReceive = kafkaTemplate.sendAndReceive(record);

		SendResult<String, String> sendResult = sendAndReceive.getSendFuture().get();

		System.out.println("reply topic: " + new String(sendResult.getProducerRecord().headers().lastHeader(KafkaHeaders.REPLY_TOPIC).value()));
		System.out.println("correlationid: " + new CorrelationKey(sendResult.getProducerRecord().headers().lastHeader(KafkaHeaders.CORRELATION_ID).value()));

		ConsumerRecord<String, String> consumerRecord = sendAndReceive.get();
		return mapper.readValue(consumerRecord.value(), Model.class);
	}
	
	@ResponseBody
	@PostMapping(value = "/sync-call", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	public Map<String, String> anotherMethod(@RequestBody Map<String, String> data) throws InterruptedException, ExecutionException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		ProducerRecord<String, String> record = new ProducerRecord<>(requestTopic2, mapper.writeValueAsString(data));
		record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, requestReplyTopic2.getBytes()));
		
		RequestReplyFuture<String, String, String> sendAndReceive = kafkaTemplate.sendAndReceive(record);
		
		SendResult<String, String> sendResult = sendAndReceive.getSendFuture().get();

		System.out.println("reply topic: " + new String(sendResult.getProducerRecord().headers().lastHeader(KafkaHeaders.REPLY_TOPIC).value()));
		System.out.println("correlationid: " + new CorrelationKey(sendResult.getProducerRecord().headers().lastHeader(KafkaHeaders.CORRELATION_ID).value()));

		ConsumerRecord<String, String> consumerRecord = sendAndReceive.get();
		
		TypeReference<HashMap<String,Object>> typeRef = new TypeReference<HashMap<String,Object>>() {};
		return mapper.readValue(consumerRecord.value(), typeRef);
	}
	
	@RequestMapping(path = "/hello", method = RequestMethod.GET)
	public String hello() {
		return "Hello World";
	}
}
