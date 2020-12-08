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
    private final static String INPUT_FILE = "selected_spotify_tracks.csv";
    private final static int ROW_LENGTH = 19;
    private final static int ID_COLUMN = 6;
    private final static int YEAR_COLUMN = 18;
    private final static int TEMPO_COLUMN = 16;
    private final static String CLIENT_ID = SpotifyProducer.class.getName();
    private static final int DELAY_BASE = 200; //ms

    private final Producer<Long, String> producer;
    private final int messageCount; // Used just for testing

 
    SpotifyProducer(Producer<Long, String> producer) {
        this.producer = producer;
        this.messageCount = -1;
    }

    SpotifyProducer(Producer<Long, String> producer, int messageCount) {
        this.producer = producer;
        this.messageCount = messageCount;
    }


    private String getRecordValue(String id, String year, String tempo) {
        return String.format("[%s,%s,%s]",
            id,
            year,
            tempo);
    }

    @Override
    public void run() {
        System.out.println("=== SPOTIFY PRODUCER ===");
        try {
            FileReader fr = new FileReader(INPUT_FILE);
            BufferedReader br = new BufferedReader(fr);

            String line;
            ProducerRecord<Long, String> record;
            long key;
            String value;
            int skipLines = 1;
            int sentMessages = 0;

            while ((line = br.readLine()) != null) {
                // Skipping column names row
                if (skipLines-- > 0) continue;

                // Exporting id, tempo and year from the row
                String[] parts = line.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                if (parts.length != ROW_LENGTH) continue;
                String tempo = parts[TEMPO_COLUMN];
                String year = parts[YEAR_COLUMN];
                String id = parts[ID_COLUMN];

                // Setting message key
                try {
                    key = Long.parseLong(year);
                } catch(NumberFormatException error) {
                    System.out.println("Wrong year - cant convert to long\n");
                    continue;
                }

                value = getRecordValue(id, year, tempo);
                record = new ProducerRecord<>(TOPIC, key, value);
                producer.send(record);
                System.out.printf("[Sent to %s] MsgKey = %d, Value = %s\n",
                        TOPIC,
                        key,
                        value);
                
                // Used only for testing
                if (messageCount != -1) {
                    if (++sentMessages == messageCount) break;
                }
                
                // Delay between records sending
                try {
                    Thread.sleep(DELAY_BASE);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            System.out.println("Closing ...");
            producer.flush();
            producer.close();

            System.out.println("Done.");
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
            System.err.println("Usage: SpotifyProducer");
            System.exit(2);
        }
    }
}
