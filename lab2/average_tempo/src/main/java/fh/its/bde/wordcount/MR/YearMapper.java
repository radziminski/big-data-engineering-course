package fh.its.bde.wordcount.MR;

import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
// import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import fh.its.bde.wordcount.WordCountApp;

public class YearMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static final Logger LOG = Logger.getLogger(WordCountApp.class);

    private Text year = new Text();
    private final static LongWritable number = new LongWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        LOG.info("Mapping started....");
        final int upperBpmBound = 160;
        final int lowerBpmBound = 80;
        final int songBpmBound = 30;

        LOG.info(key);
        int rowLength = 19;

        String[] parts = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        if (parts.length != rowLength)
            return;

        try {
            long tempo = Math.round(Double.parseDouble(parts[16]));
            if (tempo < songBpmBound)
                return;
            while (tempo >= upperBpmBound) {
                tempo = (long) tempo / 2;
            }
            while (tempo < lowerBpmBound) {
                tempo = (long) tempo * 2;
            }
            year.set(parts[18]);
            number.set(tempo);
            LOG.info(year);
            context.write(year, number);
        } catch (Exception e) {
            System.out.println("error");
            return;
        }

    }
}
