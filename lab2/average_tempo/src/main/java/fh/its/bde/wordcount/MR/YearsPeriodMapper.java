package fh.its.bde.wordcount.MR;

import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
// import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import fh.its.bde.wordcount.WordCountApp;

public class YearsPeriodMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
    private static final Logger LOG = Logger.getLogger(WordCountApp.class);

    private Text years = new Text();
    private final static LongWritable number = new LongWritable();

    public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
        LOG.info("Mapping started....");
        String k = key.toString().substring(0, 3).toString() + "0";

        years.set(k);
        LOG.info(years);
        number.set(value.get());

        context.write(years, number);
        // context.write(key, value);

    }
}
