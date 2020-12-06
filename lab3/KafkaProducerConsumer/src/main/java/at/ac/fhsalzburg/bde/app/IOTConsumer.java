package at.ac.fhsalzburg.bde.app;

import java.io.IOException;
import java.time.Duration;

// import java.util.Collections;
// import java.util.Properties;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.*;
// import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
// import org.apache.kafka.common.serialization.LongDeserializer;
// import org.apache.kafka.common.serialization.StringDeserializer;

public class IOTConsumer implements Runnable {
    private final static String CLIENTID = "IOTConsumer";
    private final static String GROUPID = "IOTConsumerGroup";
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Consumer<Long, String> consumer;


    IOTConsumer(Consumer<Long, String> consumer) {
        this.consumer = consumer;
        consumer.subscribe(Collections.singletonList(IOTProducer.TOPIC));
    }

    public void run() {
        try {
            System.out.println("|subscribed to " + IOTProducer.TOPIC);
            System.out.println("|waiting for messages...");
            while (!closed.get()) {
                ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(1000));
                System.out.println("||after poll (enter to exit); records: " + records.count());
                for (ConsumerRecord<Long, String> record : records)
                    System.out.printf(
                            "partition = %d, offset = %d, key = %s, value = %s\n",
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value());
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

    // Shutdown hook which can be called from a separate thread
    void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

    public static void main(String[] args) throws InterruptedException, IOException {

        // start consuming thread
        IOTConsumer r = new IOTConsumer(Helper.createConsumer(CLIENTID, GROUPID));
        Thread t = new Thread(r);
        t.start();

        System.out.println("press any key to exit");
        System.in.read();

        System.out.println("shutting down ...");
        r.shutdown();
        t.join();
        System.out.println("done.");
    }
}
