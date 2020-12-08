package at.ac.fhsalzburg.bde.app;

import java.io.IOException;
import java.time.Duration;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;

public class SpotifyConsumer implements Runnable {
    private final static String CLIENTID = SpotifyConsumer.class.getName();
    private final static String GROUPID = "SpotifyConsumerGroup";
    private final AtomicBoolean closed = new AtomicBoolean(false);
    public final static int POLL_DURATION_MS = 1000;

    private final Consumer<Long, String> consumer;

    SpotifyConsumer(Consumer<Long, String> consumer) {
        this.consumer = consumer;
        consumer.subscribe(Collections.singletonList(SpotifyProducer.TOPIC));
    }

    private String[] getValuesFromArray(String str) {
        String[] values = new String[0];
        if (str.length() < 6) return values;
        
        values = str.substring(1,  str.length() - 2).replace("\"", "").replace(" ", "").split(",");
        return values;
    }

    public void run() {
        System.out.println("=== SPOTIFY CONSUMER ===");
        try {
            System.out.println("=> Subscribed to " + SpotifyProducer.TOPIC);
            System.out.println("==> Waiting for messages...");
            while (!closed.get()) {
                ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(POLL_DURATION_MS));
                System.out.println("==> After poll (enter to exit); records: " + records.count());
                for (ConsumerRecord<Long, String> record : records) {
                    String[] values;
                    values = getValuesFromArray(record.value());
            
                    // Wrong record format
                    if (values.length != 3) continue;

                    System.out.printf(
                            "[Partition: %d, Offset: %d, MsgKey: %s] Id: %s, Year: %s, Tempo: %s;\n",
                            record.partition(),
                            record.offset(),
                            record.key(),
                            values[0],
                            values[1],
                            values[2]);
                }
            }
        } catch (WakeupException e) {
            System.out.println(e.toString());
            /* Ignore exception if closing */
            if (!closed.get())
                throw e;
        } finally {
            consumer.close();
        }
    }

    void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

    public static void main(String[] args) throws InterruptedException, IOException {

        SpotifyConsumer r = new SpotifyConsumer(Helper.createConsumer(CLIENTID, GROUPID));
        Thread t = new Thread(r);
        t.start();

        System.out.println("Press any key to exit");
        System.in.read();

        System.out.println("Shutting down ...");
        r.shutdown();
        t.join();
        System.out.println("Done.");
    }
}