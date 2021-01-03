package at.ac.fhsalzburg.bde.app;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class SpotifyArtistsProducer implements Runnable {
    public final static String TOPIC = "groupB04.spotifyartists";
    private final static String INPUT_FILE = "selected_spotify_tracks.csv";
    private final static int ROW_LENGTH = 19;
    private final static int ID_COLUMN = 6;
    private final static int ARTISTS_COLUMN = 1;
    private final static String CLIENT_ID = SpotifyArtistsProducer.class.getName();
    private static final int DELAY_BASE = 50; //ms

    private final Producer<Long, String> producer;
    private final int messageCount; // Used just for testing

 
    SpotifyArtistsProducer(Producer<Long, String> producer) {
        this.producer = producer;
        this.messageCount = -1;
    }

    SpotifyArtistsProducer(Producer<Long, String> producer, int messageCount) {
        this.producer = producer;
        this.messageCount = messageCount;
    }


    private String getRecordValue(String id, String artists) {
        return String.format("%s,%s", id, artists);
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

                // Exporting id, and artists from the row
                String[] parts = line.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                if (parts.length != ROW_LENGTH) continue;
                String artists = parts[ARTISTS_COLUMN];
                String id = parts[ID_COLUMN];

                artists = artists.replaceAll("\"", "");
                artists = artists.replaceAll("'", "");
                artists = artists.replaceAll(", ", ",");
                artists = artists.replaceAll(" ,", ",");
                
                if (artists.length() < 2) continue;
                artists = artists.substring(1, artists.length() - 1);

                value = getRecordValue(id, artists);
                record = new ProducerRecord<>(TOPIC, value);
                producer.send(record);
                System.out.printf("[Sent to %s] Value = %s\n",
                        TOPIC,
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
            new Thread(new SpotifyArtistsProducer(Helper.createProducer(CLIENT_ID))).start();
        } catch (NumberFormatException e) {
            System.err.println("Usage: SpotifyArtistsProducer");
            System.exit(2);
        }
    }
}
