package com.str.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.requestreply.CustomReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.JSONConverter;
import org.springframework.kafka.requestreply.KMessage;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class Config {
	@Value("${kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${kafka.topic.reply}")
	private String requestReplyTopic;

	@Bean
	public Map<String, Object> producerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JSONConverter.class);
		props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
		return props;
	}

	@Bean
	public Map<String, Object> consumerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "helloworld");
		return props;
	}

	@Bean
	public ProducerFactory<String, KMessage<Map<String, String>>> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}

	@Bean
	public KafkaTemplate<String, KMessage<Map<String, String>>> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	@Bean
	public CustomReplyingKafkaTemplate<String, KMessage<Map<String, String>>, KMessage<Map<String, String>>> replyKafkaTemplate(
			ProducerFactory<String, KMessage<Map<String, String>>> pf,
			KafkaMessageListenerContainer<String, KMessage<Map<String, String>>> container) {
		return new CustomReplyingKafkaTemplate<>(pf, container);

	}

	@Bean
	public KafkaMessageListenerContainer<String, KMessage<Map<String, String>>> replyContainer(
			ConsumerFactory<String, KMessage<Map<String, String>>> cf) {
		
		ContainerProperties containerProperties = new ContainerProperties(requestReplyTopic);
		return new KafkaMessageListenerContainer<>(cf, containerProperties);
	}

	@Bean
	public ConsumerFactory<String, KMessage<Map<String, String>>> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(consumerConfigs(), new StringDeserializer(), new JSONConverter());
	}
	
	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, KMessage<Map<String, String>>>> kafkaListenerContainerFactory() {
	  ConcurrentKafkaListenerContainerFactory<String, KMessage<Map<String, String>>> factory = new ConcurrentKafkaListenerContainerFactory<>();
	  factory.setConsumerFactory(consumerFactory());
	  factory.setReplyTemplate(kafkaTemplate());
	  return factory;
	}
}
