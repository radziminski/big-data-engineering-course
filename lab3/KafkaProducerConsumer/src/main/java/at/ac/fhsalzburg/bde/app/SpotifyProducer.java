package at.ac.fhsalzburg.bde.app;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

public class SpotifyProducer implements Runnable {
    public final static String TOPIC = "groupB04.spotifytempos";
    private final static String CLIENT_ID = SpotifyProducer.class.getName();

    private static final int DELAY_BASE = 500;

    private final Producer<Long, String> producer;

 
    SpotifyProducer(Producer<Long, String> producer) {
        this.producer = producer;
    }

    private String getJsonValue(String id, String year, String tempo) {
        return String.format("[%s,%s,%s]",
            id,
            year,
            tempo);
    }

    @Override
    public void run() {
        System.out.println("=== SONG PRODUCER ===");
        try {
            FileReader fr = new FileReader("spotify.csv");
            BufferedReader br = new BufferedReader(fr);


            int rowLength = 19;
            String line;
            ProducerRecord<Long, String> record;
            long key;
            String value;
            int skipLines = 1;

            while ((line = br.readLine()) != null) {
                if (skipLines-- > 0) continue;

                // just to get different years
                for (int i = 0; i < 100; i++) br.readLine();

                String[] parts = line.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                if (parts.length != rowLength) continue;
                String tempo = parts[18];
                String year = parts[1];
                String id = parts[8];

                try {
                    key = Long.parseLong(year);
                } catch(NumberFormatException error) {
                    System.out.println("Wrong year - cant convert to long\n");
                    continue;
                }

                value = getJsonValue(id, year, tempo);

                record = new ProducerRecord<>(TOPIC, key, value);
                producer.send(record);

                System.out.printf("[sent to %s topic] MsgKey = %d, Value = %s\n",
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
        } catch(FileNotFoundException e) {
            System.out.println("File not found");
            return;
        } catch(IOException e) {
            System.out.println("IO exception");
            return;
        }
        
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
