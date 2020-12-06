package at.ac.fhsalzburg.bde.app;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * produce email text
 */
public class EmailProducer {
    private final static String CLIENT_ID = "EmailProducer";
    private final static String TOPIC = "email";

    public static void main(String[] otherArgs) throws InterruptedException {
        if (otherArgs.length != 2) {
            System.err.println("Usage: EMailProducer <inputfile> <intervall [ms]>");
            System.exit(2);
        }

        long interval = Long.parseLong(otherArgs[1]);

        final Producer<Long, String> producer =
            Helper.createProducer(CLIENT_ID);

        System.out.println("start sending...");
        ProducerRecord<Long, String> record;

        try {
            // pass the path to the file as a parameter
            FileReader fr = new FileReader(otherArgs[0]);
            BufferedReader br = new BufferedReader(fr);

            // read line by line
            String line;
            while ((line = br.readLine()) != null) {
                record = new ProducerRecord<>(TOPIC, System.currentTimeMillis(), line);

                System.out.printf("sending %s\n", line);

                producer.send(record);

                Thread.sleep(interval);
            }
        } catch (IOException e) {
            System.err.format("IOException: %s%n", e);
        }


        System.out.println("closing ...");
        producer.close();

        System.out.println("done.");
    }
}
