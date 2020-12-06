package at.ac.fhsalzburg.bde.app;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.util.LinkedList;
import java.util.Random;
import java.util.UUID;

import static java.lang.Thread.sleep;
import static at.ac.fhsalzburg.bde.app.Helper.getFromEnv;

public class IOTProducerAvro implements Runnable {
    public final static String PRODUCER_SEND_MESSAGES_COUNT = "PRODUCER_SEND_MESSAGES_COUNT";
    private final static String SEND_MESSAGES_COUNT_STR = System.getenv(PRODUCER_SEND_MESSAGES_COUNT);

    public final static String TOPIC = "groupB04_TEST";;
    private final static String CLIENT_ID = IOTProducerAvro.class.getName();

    private final static int SENSOR_COUNT = 10;
    private final static int SEND_MESSAGES_COUNT = Helper.isNull(SEND_MESSAGES_COUNT_STR, 1000);
    private static final int DELAY_BASE_FOR_RANDOM = 30;

    private final int sendMessageCount;
    private final Producer<Long, SensorValue> producer;
    private final Random sensorRandom = new Random(System.currentTimeMillis());
    private final LinkedList<UUID> sensorIDs = new LinkedList<UUID>() {{
        for (int i = 0; i < SENSOR_COUNT; i++) add(UUID.randomUUID());
    }};

    IOTProducerAvro(int sendMessageCount, Producer<Long, SensorValue> producer) {
        this.producer = producer;
        this.sendMessageCount = sendMessageCount;
    }

    private String selectSensor() {
        return sensorIDs.get(sensorRandom
                .nextInt(SENSOR_COUNT - 1)).toString();
    }

    private SensorValue getSensorValue(Long i) {
        return new SensorValue(
                i,
                selectSensor(),
                sensorRandom.nextDouble() * sensorRandom.nextInt(10)
        );
    }

    private int getSendMessageCount() {
        return sendMessageCount;
    }

    @Override
    public void run() {
        System.out.println("=== PRODUCER AVRO");
        System.out.println("start sending...");
        try {
            for (int i = 0; i < getSendMessageCount(); i++) {
                final SensorValue sensorValue = getSensorValue(Integer.valueOf(i).longValue());
                final ProducerRecord<Long, SensorValue> record =
                        new ProducerRecord<>(TOPIC, sensorValue.getId(), sensorValue);
                producer.send(record);

                System.out.printf("sent record topic = %s, key = %s, value = %f%n",
                        TOPIC,
                        sensorValue.getId(),
                        sensorValue.getValue());

                // random sleep period
                sleep(DELAY_BASE_FOR_RANDOM * sensorRandom.nextInt(SENSOR_COUNT));
            }

            System.out.println("closing ...");
            producer.flush();
            producer.close();
            System.out.printf("Successfully produced %d messages to a topic called %s\n",
                    getSendMessageCount(), TOPIC);

        } catch (final SerializationException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        try {
            new Thread(new IOTProducerAvro(
                    (args.length == 1) ? Integer.parseInt(args[0]) : SEND_MESSAGES_COUNT,
                    Helper.createAvroProducer(CLIENT_ID)))
                    .start();
        } catch (NumberFormatException e) {
            System.err.println("Usage: IOTProducerAvro [message_count]");
            System.exit(2);
        }
    }
}

