package fh.its.bde.wordcount.MR;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class YearsPeriodReducer extends Reducer<Text, LongWritable, Text, DoubleWritable> {
  private DoubleWritable result = new DoubleWritable();

  private static final Logger LOG = Logger.getLogger(YearReducer.class);

  public void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    LOG.info("Reducing...");
    long sum = 0;
    int count = 0;
    for (LongWritable val : values) {
      LOG.info("val...");
      count++;
      sum += val.get();
    }
    if (count == 0)
      return;
    double val = Double.valueOf(sum / count);
    result.set(val);
    context.write(key, result);
  }
}
