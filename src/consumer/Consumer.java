package consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import consumer_parse.JSONParse;

public class Consumer {
	private static Logger logger = LoggerFactory.getLogger(Consumer.class);
	public static KafkaConsumer<String, String> consumer;

	public static KafkaConsumer<String, String> setupConsumer(String filePath) {
		if (consumer != null)
			return consumer;
		JSONObject config = JSONParse.parseJSON(filePath);
		logger.info("Config loaded from give file path " + filePath);
		logger.info("config "+config);
		List<String> topics = JSONParse.getTopics(config);
		logger.info("topics "+topics.toString());
		List<String> brokers = JSONParse.getBrokerServers(config);
		logger.info("brokers "+brokers.toString());
		String consumerGroupId = JSONParse.getConsumerGroupId(config);
		logger.info("consumerGroupId "+consumerGroupId);
		if (brokers == null || topics.size() == 0) {
			// selecting the default broker server localhost:9092
			brokers = Arrays.asList("localhost:9092");
		}
		if (topics == null || topics.size() == 0) {
			// selecting a default topic to listen to
			topics = Arrays.asList("quickstart-events");
		}
		if (consumerGroupId == null || consumerGroupId.strip().equals("")) {
			// selecting a default consumerGroupId
			consumerGroupId = "consumer-group";
		}
		logger.info("Consuming the message, Consumer Group Id: " + consumerGroupId + ", brokers: " + brokers
				+ ", topics: " + topics);
		Properties props = getConsumerProperties(brokers, consumerGroupId);
		consumer = new KafkaConsumer<>(props);
		consumer.subscribe(topics);
		return consumer;
	}

	public static Properties getConsumerProperties(List<String> brokerServers, String consumerGroupId) {
		Properties props = new Properties();
		String servers = brokerServers.stream().map(String::toUpperCase).collect(Collectors.joining(","));
		props.put("bootstrap.servers", servers);
		props.put("group.id", consumerGroupId);
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		return props;
	}

	public static List<String> consume(String filePath) {
		List<String> result = new ArrayList<>();
		KafkaConsumer<String, String> consumer = null;
		try {
			consumer = Consumer.setupConsumer(filePath);
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Integer.MAX_VALUE));
			for (ConsumerRecord<String, String> record : records) {
				result.add(record.value());
			}
		} catch (Exception e) {
			logger.error("Error occured while consuming the message " + e.getMessage());
		} 
		return result;
	}
	
	public static void closeConsumer() {
		if(consumer != null) consumer.close();
	}

	public static void main(String[] args) {
		String filePath = "src/config.json";
		if (args.length == 0) {
			logger.info("Config file path arguments not provided, using default filePath : " + filePath);
		} else {
			filePath = args[0];
		}
		try {
			KafkaConsumer<String, String> consumer = Consumer.setupConsumer(filePath);
			try {
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
					for (ConsumerRecord<String, String> record : records) {
						Map<String, Object> data = new HashMap<>();
						data.put("partition", record.partition());
						data.put("offset", record.offset());
						data.put("value", record.value());
						System.out.println(data);
					}
				}
			} catch (WakeupException e) {
				// ignore for shutdown
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error occured while starting the consumer " + e.getMessage());
		}
	}
}
