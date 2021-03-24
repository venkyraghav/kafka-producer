package com.venkyraghav.confluent.kafka;

import com.venkyraghav.confluent.kafka.util.ParseCommandLine;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.FileSystems;
import java.util.*;

public class KafkaProducerApplication {
	final private static Logger log = LoggerFactory.getLogger(KafkaProducerApplication.class);

	final static private String TOPIC = "topic";
	final static private String MESSAGE = "message";
	final static private String NUMMSG = "num-mesg";
	final static private String DELAY = "delay";
	final static private String STARTSEQ = "start-seq";
	final static private String INCREMENT = "increment";
	final static private String CLIENTCFG = "client.config";

	final private Options options = new Options();

	public static void main(String[] args) {
		KafkaProducerApplication kafkaProducerApplication = new KafkaProducerApplication();
		Properties properties = kafkaProducerApplication.processCommandLine(args);
		if (properties == null) {
			System.exit(1);
		}
		kafkaProducerApplication.run(properties);
	}

	private KafkaProducer<Integer, Object> createProducer(Properties properties) {
		if (properties.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG) == false) {
			properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
		}
		if (properties.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG) == false) {
			properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		}

		// properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, CHANGE_TRANSACTION_ID);

		return new KafkaProducer<Integer, Object>(properties);
	}

	public void run(Properties properties) {
		KafkaProducer<Integer, Object> producer = createProducer(properties);
		String topic = properties.get(KafkaProducerApplication.TOPIC).toString();
		String message = properties.get(KafkaProducerApplication.MESSAGE).toString();
		int numMessages = Integer.parseInt(properties.get(KafkaProducerApplication.NUMMSG).toString());
		int delay = Integer.parseInt(properties.get(KafkaProducerApplication.DELAY).toString());
		int startSequence = Integer.parseInt(properties.get(KafkaProducerApplication.STARTSEQ).toString());
		int increment = Integer.parseInt(properties.get(KafkaProducerApplication.INCREMENT).toString());

		for (int i = startSequence;i < startSequence+numMessages;) {
			ProducerRecord<Integer, Object> record =
					new ProducerRecord<Integer, Object> (topic, Integer.valueOf(i), message + " is " + i);
			producer.send(record, (metadata, exception) -> {
				if (exception != null) {
					exception.printStackTrace();
				} else {
					if (log.isInfoEnabled()) {
						log.info("Message " + record.key() + ", " + record.value());
					}
				}
			});
			i = i + increment;
			try {
				Thread.sleep(delay);
			} catch (InterruptedException e) {
				if (log.isDebugEnabled()) {log.debug(e.getMessage());}
			}
		}
		producer.close();
	}

	private String checkRequiredFields(Properties properties) {
		if (properties.get(KafkaProducerApplication.TOPIC) == null || properties.get(KafkaProducerApplication.TOPIC).toString().isEmpty()) {
			return KafkaProducerApplication.TOPIC;
		}
		if (properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) == null || properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).toString().isEmpty()) {
			return ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
		}
		return null;
	}

	private Properties processCommandLine(String[] args) {
		setupOptions();

		ParseCommandLine parseCommandLine = new ParseCommandLine();
		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();

		CommandLine cmd;
		try {
			cmd = parser.parse(options, args);

			if (parseCommandLine.getNoValueArg("KafkaProducer", cmd, options, "h", "help") == true) {
				System.out.println("Show Usage");
				formatter.printHelp("KafkaProducer", options);
				System.exit(1);
			}
			if (parseCommandLine.getNoValueArg("KafkaProducer", cmd, options, "g", "generate") == true) {
				generateClientConfig();
				System.exit(1);
			}

			Optional<String> bootstrapServer = parseCommandLine.getStringArg("KafkaProducer", cmd, options, "b", ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
			Optional<String> clientConfig = parseCommandLine.getStringArg("KafkaProducer", cmd, options, "c", KafkaProducerApplication.CLIENTCFG, "");
			Optional<String> message = parseCommandLine.getStringArg("KafkaProducer", cmd, options, "m", KafkaProducerApplication.MESSAGE, "Message is");
			OptionalInt delay = parseCommandLine.getIntArg("KafkaProducer", cmd, options, "d", KafkaProducerApplication.DELAY, 2000);
			OptionalInt numMessages = parseCommandLine.getIntArg("KafkaProducer", cmd, options, "n", KafkaProducerApplication.NUMMSG, 10);
			OptionalInt startSequence = parseCommandLine.getIntArg("KafkaProducer", cmd, options, "s", KafkaProducerApplication.STARTSEQ, 0);
			OptionalInt increment = parseCommandLine.getIntArg("KafkaProducer", cmd, options, "i", KafkaProducerApplication.INCREMENT, 1);
			Optional<Boolean> user = parseCommandLine.getBooleanArg("KafkaProducer", cmd, options, "u", "user", false);
			Optional<String> topic = parseCommandLine.getStringArg("KafkaProducer", cmd, options, "t", KafkaProducerApplication.TOPIC, "");

			Properties properties = new Properties();
			if (!clientConfig.isEmpty() && !clientConfig.get().isEmpty()) {
				try (InputStream input = new FileInputStream(clientConfig.get().toString())) {
					properties.load(input);
				} catch (IOException e) {
					System.out.println(e.getMessage());
					System.exit(-1);
				}
			}
			if (bootstrapServer.get().isEmpty() == false) {
				if (log.isInfoEnabled()) {log.info("Using " + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + "=" + bootstrapServer.get() + " from command line");}
				properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer.get());
			} else {
				if (log.isInfoEnabled()) {log.info("Using " + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + "=" + properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));}
			}
			properties.put(KafkaProducerApplication.MESSAGE, message.get());
			properties.put(KafkaProducerApplication.DELAY, String.valueOf(delay.getAsInt()));
			properties.put(KafkaProducerApplication.NUMMSG, String.valueOf(numMessages.getAsInt()));
			properties.put(KafkaProducerApplication.STARTSEQ, String.valueOf(startSequence.getAsInt()));
			properties.put(KafkaProducerApplication.INCREMENT, String.valueOf(increment.getAsInt()));
			properties.put("user", user.get());
			properties.put(KafkaProducerApplication.TOPIC, topic.get());

			String field = checkRequiredFields(properties);
			if (field != null) {
				parseCommandLine.showHelp("KafkaProducer", options, "", field, "", field + " is required");
				System.exit(-1);
			}

			return properties;
		} catch (ParseException e) {
			System.out.println(e.getMessage());
			parseCommandLine.showHelp("KafkaProducer", options, "", "", "", e.getMessage());
		}
		return null;
	}

	private void generateGeneralConfig(FileWriter writer) throws IOException {
		writer.write("## " + System.lineSeparator());
		writer.write("## Generated by KafkaProducerApplication" + System.lineSeparator());
		writer.write("## " + System.lineSeparator());
		writer.write("## " + System.lineSeparator());
		writer.write("# General Config" + System.lineSeparator());
		writer.write("## " + System.lineSeparator());
		writer.write("# " + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + "=localhost:9092" + System.lineSeparator());
		writer.write("# " + "schema.registry.url" + "=localhost:8081" + System.lineSeparator());
		writer.write("# " + ProducerConfig.CLIENT_ID_CONFIG + "=TestClient" + System.lineSeparator());
		writer.write("# " + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG + "=" + System.lineSeparator());
		writer.write("# " + ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG + "=" + System.lineSeparator());
		writer.write("# security.protocol=SASL_SSL" + System.lineSeparator());
		writer.write("# ssl.truststore.location=/var/ssl/private/kafka.client.truststore.jks" + System.lineSeparator());
		writer.write("# ssl.truststore.password=test1234" + System.lineSeparator());
		writer.write("# sasl.mechanism=PLAIN" + System.lineSeparator());
		writer.write("# sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"client\" password=\"client-secret\";" + System.lineSeparator());
		writer.write("## " + System.lineSeparator());
	}

	private void generateProducerConfig(FileWriter writer) throws IOException {
		writer.write("## " + System.lineSeparator());
		writer.write("## Producer Config" + System.lineSeparator());
		writer.write("## " + System.lineSeparator());
		writer.write("# " + ProducerConfig.ACKS_CONFIG + "=1" + System.lineSeparator());
		writer.write("# " + ProducerConfig.BATCH_SIZE_CONFIG + "=0" + System.lineSeparator());
		writer.write("# " + ProducerConfig.COMPRESSION_TYPE_CONFIG + "=none" + System.lineSeparator());
		writer.write("# " + ProducerConfig.LINGER_MS_CONFIG + "=10000" + System.lineSeparator());
		writer.write("# " + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG + "=org.apache.kafka.common.serialization.StringSerializer" + System.lineSeparator());
		writer.write("# " + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG + "=org.apache.kafka.common.serialization.StringSerializer" + System.lineSeparator());
		writer.write("# " + ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG + "=false" + System.lineSeparator());
		writer.write("# " + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + "=" + System.lineSeparator());
		writer.write("# " + ProducerConfig.MAX_REQUEST_SIZE_CONFIG + "=" + System.lineSeparator());
		writer.write("# " + ProducerConfig.PARTITIONER_CLASS_CONFIG + "=" + System.lineSeparator());
		writer.write("# " + ProducerConfig.RETRIES_CONFIG + "=" + System.lineSeparator());
		writer.write("# " + ProducerConfig.RETRY_BACKOFF_MS_CONFIG + "=" + System.lineSeparator());
		writer.write("# " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + "=" + System.lineSeparator());
	}

	private void generateConsumerConfig(FileWriter writer) throws IOException {
		writer.write("## " + System.lineSeparator());
		writer.write("## Consumer Config" + System.lineSeparator());
		writer.write("## " + System.lineSeparator());
		writer.write("# " + ConsumerConfig.GROUP_ID_CONFIG + "=TestConsumerGroup" + System.lineSeparator());
		writer.write("# " + ConsumerConfig.GROUP_INSTANCE_ID_CONFIG + "=TestConsumerInstance" + System.lineSeparator());
		writer.write("# " + ConsumerConfig.CLIENT_RACK_CONFIG + "=" + System.lineSeparator());
		writer.write("# " + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG + "=" + System.lineSeparator());
		writer.write("# " + ConsumerConfig.FETCH_MAX_BYTES_CONFIG + "=" + System.lineSeparator());
		writer.write("# " + ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG + "=" + System.lineSeparator());
		writer.write("# " + ConsumerConfig.FETCH_MIN_BYTES_CONFIG + "=" + System.lineSeparator());
		writer.write("# " + ConsumerConfig.ISOLATION_LEVEL_CONFIG + "=" + System.lineSeparator());
		writer.write("# " + ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG + "=org.apache.kafka.common.serialization.StringDeserializer" + System.lineSeparator());
		writer.write("# " + ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG + "=org.apache.kafka.common.serialization.StringDeserializer" + System.lineSeparator());
		writer.write("# " + ConsumerConfig.MAX_POLL_RECORDS_CONFIG + "=" + System.lineSeparator());
		writer.write("# " + ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG + "=" + System.lineSeparator());
	}

	private void generateClientConfig() {
		String tmpDir = System.getProperty("java.io.tmpdir");
		String clientConfigFile = tmpDir + FileSystems.getDefault().getSeparator() + "KafkaProducerApplication.properties";
		try {
			FileWriter writer = new FileWriter(clientConfigFile);
			generateGeneralConfig(writer);
			generateProducerConfig(writer);
			generateConsumerConfig(writer);
			writer.close();
			if (log.isInfoEnabled()) {log.info("Generate Client Config Template " + clientConfigFile);}
			System.exit(0);
		} catch (IOException e) {
			log.error("File: " + clientConfigFile, e);
			System.exit(-1);
		}
	}

	public void setupOptions() {
		Option optGenerate = new Option("g", "generate", false, "Generate Client Config template with defaults");
		optGenerate.setRequired(false);
		options.addOption(optGenerate);

		Option optClientConfig = new Option("c", KafkaProducerApplication.CLIENTCFG, true, "Client Configuration File");
		optClientConfig.setRequired(false);
		options.addOption(optClientConfig);

		Option optBootstrapServer = new Option("b", ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, true, "Bootstrap Server. Overrides setting in Client Config file");
		optBootstrapServer.setRequired(false);
		options.addOption(optBootstrapServer);

		Option optMessage = new Option("m", KafkaProducerApplication.MESSAGE, true, "Test Message. Default Message is");
		optMessage.setRequired(false);
		options.addOption(optMessage);

		Option optDelay = new Option("d", KafkaProducerApplication.DELAY, true, "Delay between messages. Default 2s");
		optDelay.setRequired(false);
		options.addOption(optDelay);

		Option optNumMessage = new Option("n", KafkaProducerApplication.NUMMSG, true, "# of messages. Default 10");
		optNumMessage.setRequired(false);
		options.addOption(optNumMessage);

		Option optStartSeq = new Option("s", KafkaProducerApplication.STARTSEQ, true, "Start Sequence Number. Default 0");
		optStartSeq.setRequired(false);
		options.addOption(optStartSeq);

		Option optIncrement = new Option("i", KafkaProducerApplication.INCREMENT, true, "Increment Sequence. Default 1");
		optIncrement.setRequired(false);
		options.addOption(optIncrement);

		Option optUser = new Option("u", "user", false, "Use User Data for message");
		optUser.setRequired(false);
		options.addOption(optUser);

		Option optHelp = new Option("h", "help", false, "Shows this help");
		optHelp.setRequired(false);
		options.addOption(optHelp);

		Option optTopic = new Option("t", KafkaProducerApplication.TOPIC, true, "Topic Name");
		optTopic.setRequired(false);
		options.addOption(optTopic);
	}
}
