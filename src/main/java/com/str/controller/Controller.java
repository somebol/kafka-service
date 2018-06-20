package com.str.controller;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.kafka.requestreply.CorrelationKey;
import org.springframework.kafka.requestreply.CustomReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.KMessage;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {
	@Autowired
	CustomReplyingKafkaTemplate<String, KMessage<Map<String, String>>, KMessage<Map<String, String>>> kafkaTemplate;

	@Value("${kafka.topic.request}")
	String requestTopic2;
	
	@Value("${kafka.topic.reply}")
	String requestReplyTopic2;
	
	@ResponseBody
	@PostMapping(value = "/sync-call", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	public Map<String, String> anotherMethod(@RequestBody Map<String, String> data) throws InterruptedException, ExecutionException, IOException {
		KMessage<Map<String, String>> message = new KMessage<>();
		message.setValue(data);
		
		ProducerRecord<String, KMessage<Map<String, String>>> record = new ProducerRecord<>(requestTopic2, message);
		record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, requestReplyTopic2.getBytes()));
		
		RequestReplyFuture<String, KMessage<Map<String, String>>, KMessage<Map<String, String>>> sendAndReceive = kafkaTemplate.sendAndReceive(record);
		
		SendResult<String, KMessage<Map<String, String>>> sendResult = sendAndReceive.getSendFuture().get();

		System.out.println("reply topic: " + new String(sendResult.getProducerRecord().headers().lastHeader(KafkaHeaders.REPLY_TOPIC).value()));
		System.out.println("correlationid: " + new CorrelationKey(sendResult.getProducerRecord().headers().lastHeader(KafkaHeaders.CORRELATION_ID).value()));

		ConsumerRecord<String, KMessage<Map<String, String>>> consumerRecord = sendAndReceive.get();
		
		return consumerRecord.value().getValue();
	}
	
	@RequestMapping(path = "/hello", method = RequestMethod.GET)
	public String hello() {
		return "Hello World";
	}
}
