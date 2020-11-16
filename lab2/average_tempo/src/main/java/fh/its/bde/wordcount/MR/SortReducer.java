package fh.its.bde.wordcount.MR;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		for (Text val : values) {
			context.write(val, key);
		}

	}
}
