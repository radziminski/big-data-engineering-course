package at.ac.fhsalzburg.bde.app;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.IOException;

/**
 * Unit test for simple App.
 */
public class SpotifyKafkaTest
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public SpotifyKafkaTest(String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( SpotifyKafkaTest.class );
    }

    /**
     * test producer
     */
    public void testProducerApp() throws InterruptedException {
        int message_count = 10;
        DummyProducer<Long, String> dp = new DummyProducer<>();
        Thread t = new Thread(new SpotifyProducer(dp, message_count));
        t.start();
        t.join();

        assertEquals(dp.getSendCount(), message_count);
    }

    /**
     * test consumer
     */
    public void testConsumer() throws IOException, InterruptedException {
        DummyConsumer dc = new DummyConsumer();
        // start consuming thread
        SpotifyConsumer r = new SpotifyConsumer(dc);
        Thread t = new Thread(r);
        t.start();

        // Wait 4 iterations
        // each iteration last one second
        int POLL_COUNT = 4;
        long WAIT_MILLIS = SpotifyConsumer.POLL_DURATION_MS * POLL_COUNT;
        Thread.sleep(WAIT_MILLIS);

        System.out.println("shutting down ...");
        r.shutdown();
        t.join();
        System.out.println("done.");

        assertEquals(dc.valueCalledCount, (dc.pollCalledCount * dc.allRecords.count()));
    }

    /**
     * test isNull if value is ok
     */
    public void testIsNullPickValue() {
        Integer val = 10;
        String strVal = "15";
        Integer res = Helper.isNull(strVal, val);

        assertEquals(Integer.valueOf(strVal), res);
    }

    /**
     * test isNull if value is not ok
     */
    public void testIsNull() {
        Integer val = 10;
        Integer res = Helper.isNull("ab", val);

        assertEquals(val, res);
    }
}
