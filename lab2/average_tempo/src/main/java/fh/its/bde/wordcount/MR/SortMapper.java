package fh.its.bde.wordcount.MR;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<Text, LongWritable, LongWritable, Text> {

	public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {

		context.write(value, key);
	}
}
