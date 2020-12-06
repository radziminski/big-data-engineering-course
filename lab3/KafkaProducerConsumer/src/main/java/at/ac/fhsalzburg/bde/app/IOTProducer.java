package at.ac.fhsalzburg.bde.app;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

/**
 * Producer for Random IOT values
 */
public class IOTProducer implements Runnable {
    private final static int SEND_MESSAGES_COUNT = 10;
    // private final static int SEND_MESSAGES_COUNT =
    //         Helper.getFromEnv(Helper.PRODUCER_SEND_MESSAGES_COUNT, 1000);
    public final static String TOPIC = "groupB04_TEST";
    private final static String CLIENT_ID = IOTProducer.class.getName();

    private final static int SENSOR_COUNT = 10;
    private static final int SENSOR_VALUE_RANGE = 20;
    private static final int DELAY_BASE_FOR_RANDOM = 100;

    private final int sendMessageCount;
    private final Producer<Long, String> producer;
    private final Random random = new Random(System.currentTimeMillis());
    private final ArrayList<Integer> sensorIDs = new ArrayList<Integer>() {{
        for (int i = 0; i < SENSOR_COUNT; i++) add(i * random.nextInt());
    }};

    IOTProducer(int sendMessageCount, Producer<Long, String> producer) {
        this.producer = producer;
        this.sendMessageCount = sendMessageCount;
    }

    private String selectSensor() {
        return sensorIDs.get(random
                .nextInt(SENSOR_COUNT - 1)).toString();
    }

    private String getSensorValue(long time) {
        // create json string
        return String.format("{ \"UUID\": \"%s\", \"value\": \"%s\", \"recorded_time\": \"%s\" }",
                    selectSensor(),
                    random.nextDouble() * SENSOR_VALUE_RANGE,
                    time);
    }

    private int getSendMessageCount() {
        return sendMessageCount;
    }

    @Override
    public void run() {
        System.out.println("=== PRODUCER NOT AVRO");
        System.out.println("start sending...");
        ProducerRecord<Long, String> record;
        long time;
        String sensorValue;

        for (int i = 0; i < getSendMessageCount(); i++) {
            time = System.currentTimeMillis();
            sensorValue = getSensorValue(time);
            record = new ProducerRecord<>(TOPIC, time, sensorValue);
            producer.send(record);

            System.out.printf("sent record topic = %s, key = %d, value = %s\n",
                    TOPIC,
                    time,
                    sensorValue);

            try {
                Thread.sleep(DELAY_BASE_FOR_RANDOM * random.nextInt(10));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("closing ...");
        producer.flush();
        producer.close();

        System.out.println("done.");
    }

    public static void main(String[] args) {
        try {
            new Thread(new IOTProducer(
                    (args.length == 1) ? Integer.parseInt(args[0]) : SEND_MESSAGES_COUNT,
                    Helper.createProducer(CLIENT_ID)))
                    .start();
        } catch (NumberFormatException e) {
            System.err.println("Usage: IOTProducer <message_count>");
            System.exit(2);
        }
    }
}
