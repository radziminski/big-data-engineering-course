package fh.its.bde.wordcount.MR;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<Text, DoubleWritable, DoubleWritable, Text> {

	public void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {

		context.write(value, key);
	}
}
