package fh.its.bde.wordcount.MR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class YearReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
  private LongWritable result = new LongWritable();

  private static final Logger LOG = Logger.getLogger(YearReducer.class);

  public void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    LOG.info("Reducing...");
    List<Long> list = new ArrayList<Long>();

    // Converting values to list
    for (LongWritable val : values) {
      list.add(val.get());
    }

    if (list.size() < 1)
      return;

    // Sorting the values
    Collections.sort(list);

    // Getting median
    long res = list.get((int) list.size() / 2);

    result.set(res);
    LOG.info(result.toString());
    // LOG.info("Key: " + key + ", result: " + result + ", avg: " + avg);
    context.write(key, result);
  }
}
