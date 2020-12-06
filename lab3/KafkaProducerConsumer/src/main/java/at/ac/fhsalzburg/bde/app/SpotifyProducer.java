package at.ac.fhsalzburg.bde.app;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

public class SpotifyProducer implements Runnable {
    public final static String TOPIC = "groupB04.spotifytest";
    private final static String CLIENT_ID = SpotifyProducer.class.getName();

    private static final int DELAY_BASE = 1000;

    private final Producer<Long, String> producer;

 
    SpotifyProducer(Producer<Long, String> producer) {
        this.producer = producer;
    }

    private String getJsonValue(int id, int tempo, int year) {
        return String.format("[%d,%d,%d]",
            id,
            year,
            tempo);
    }

    @Override
    public void run() {
        System.out.println("=== SONG PRODUCER ===");

        ProducerRecord<Long, String> record;
        long key;
        String value;

        for (int i = 0; i < 30; i++) {
            key = i % 10;
            value = getJsonValue(i, 128 + i, 1921 + i*10);
            record = new ProducerRecord<>(TOPIC, key, value);
            producer.send(record);

            System.out.printf("sent record topic = %s, key = %d, value = %s\n",
                    TOPIC,
                    key,
                    value);

            try {
                Thread.sleep(DELAY_BASE);
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
            new Thread(new SpotifyProducer(Helper.createProducer(CLIENT_ID))).start();
        } catch (NumberFormatException e) {
            System.err.println("Usage: SpotifyProducer <message_count>");
            System.exit(2);
        }
    }
}
