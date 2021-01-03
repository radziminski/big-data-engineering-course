package at.ac.fhsalzburg.bde.app;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.FileAlreadyExistsException;
import java.text.ParseException;
import java.util.Collections;
import java.util.Properties;

public class Helper {
    // environment variables name list
    public final static String KAFKA_BOOTSTRAP_SERVERS = "KAFKA_BOOTSTRAP_SERVERS";
    public final static String KAFKA_DEFAULT_TOPIC = "KAFKA_DEFAULT_TOPIC";
    public final static String SCHEMA_REGISTRY_URL_CONFIG = "SCHEMA_REGISTRY_URL_CONFIG";
    public final static String PRODUCER_SEND_MESSAGES_COUNT = "PRODUCER_SEND_MESSAGES_COUNT";

    // environment variable set/read
    // static set variables
    public final static String BOOTSTRAP_SERVERS_LAB_ENV = "node1:9092, node2:9092, node3:9092, node4:9092";
    // public final static String BOOTSTRAP_SERVERS_LAB_ENV = "node1.bde.fh-salzburg.ac.at:9092, node2.bde.fh-salzburg.ac.at:9092, node3.bde.fh-salzburg.ac.at:9092, node4.bde.fh-salzburg.ac.at:9092";
    public final static String BOOTSTRAP_SERVERS_LOCAL = "localhost:9092";

    // read system environment variables
    public final static String BOOTSTRAP_SERVERS =
            getFromEnv(KAFKA_BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS_LAB_ENV);
    public final static String DEFAULT_REGISTRY_URL_CONFIG =
            getFromEnv(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

    public static Producer<Long, String> createProducer(String clientID, String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        props.put("acks", "all");
        props.put("delivery.timeout.ms", 40000);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);

        return new KafkaProducer<>(props);
    }

    public static Producer<Long, String> createProducer(String clientID) {
        String brokersList = BOOTSTRAP_SERVERS;
        if (brokersList==null || "".equals(brokersList)) brokersList = BOOTSTRAP_SERVERS_LAB_ENV;

        System.out.println("Using brokers: " + brokersList);    
        return createProducer(clientID, brokersList);
    }


    /**
     * create a consumer
     *
     * @param clientID
     * @param groupId
     * @param bootstrapServers
     * @return
     */
    private static KafkaConsumer<Long, String> createConsumer(String clientID, String groupId, String bootstrapServers) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientID);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        //new consumer
        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    /**
     * template for creating a Kafka Consumer
     *
     * @param clientID
     * @param groupID
     * @return a new Consumer
     */
    public static KafkaConsumer<Long, String> createConsumer(String clientID, String groupID) {
        return createConsumer(clientID, groupID, BOOTSTRAP_SERVERS_LAB_ENV);
    }

    /**
     * read value and convert to destination
     * type or provide default value
     *
     * @param value as string
     * @param defaultValue declared as destination type
     * @param <D> destination type
     * @return converted string value or give default value
     */
    public static <D> D isNull(String value, D defaultValue) {
        if (value == null || value.isEmpty()) return defaultValue;
        if (value.getClass() == defaultValue.getClass()) return (D) value;

        try {
            Class<?> c = defaultValue.getClass();
            return (D) c.getMethod("valueOf", String.class).invoke(c, value);
        } catch (NoSuchMethodException | InvocationTargetException |
                IllegalAccessException | NumberFormatException e) {
            //e.printStackTrace();
        }

        return defaultValue;
    }

    /**
     * gets value from environment variable or returns
     * provided default value
     *
     * @param value env var name
     * @param defaultValue value if env var is not set
     * @param <D> return type
     * @return
     */
    public static <D> D getFromEnv(String value, D defaultValue) {
        String envVal = System.getenv(value);

        return isNull(envVal, defaultValue);
    }

    public static Properties getProperties(String name) throws IOException {
        String fileName = name.endsWith(".properties")?name:name+".properties";

        try (InputStream input = Helper.class.getClassLoader().getResourceAsStream(fileName)) {
            if (input == null) {
                throw new FileNotFoundException("Sorry, unable to find " + fileName);
            }

            //load a properties file from class path, inside static method
            Properties prop = new Properties();
            prop.load(input);

            return prop;
        }
    }
}
