package fh.its.bde.wordcount;

import org.apache.hadoop.io.DoubleWritable;
// import jdk.internal.jline.internal.Log;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;

import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import fh.its.bde.wordcount.MR.*;
import org.apache.log4j.Logger;

public class TempoAverage {
	private static final Logger LOG = Logger.getLogger(TempoAverage.class);
	private static final int HEALTH_CHECK_INTERVAL = 5;

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] parsedArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (parsedArgs.length != 2) {
			LOG.error("Usage: AverageTempo <in> <out>");
			System.exit(2);
		}
		String fileIn = parsedArgs[0];
		String fileOut = parsedArgs[1];
		String fileOutTemp = fileOut + "temp_map";
		String fileOutTemp2 = fileOut + "temp_sort";

		// Year median job
		Job jobTempoMedian = Job.getInstance(conf, "Average Tempo - year median job");
		jobTempoMedian.setJarByClass(TempoAverage.class);
		jobTempoMedian.setMapperClass(YearMapper.class);
		jobTempoMedian.setReducerClass(YearReducer.class);

		// three worker nodes
		jobTempoMedian.setNumReduceTasks(3);

		jobTempoMedian.setOutputKeyClass(Text.class);
		jobTempoMedian.setOutputValueClass(LongWritable.class);

		jobTempoMedian.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(jobTempoMedian, new Path(fileIn));
		FileOutputFormat.setOutputPath(jobTempoMedian, new Path(fileOutTemp));

		// Years average job
		Job jobYearsAvg = Job.getInstance(conf, "Average Tempo - years average job");
		jobYearsAvg.setJarByClass(TempoAverage.class);
		jobYearsAvg.setMapperClass(YearsPeriodMapper.class);
		jobYearsAvg.setReducerClass(YearsPeriodReducer.class);

		jobYearsAvg.setOutputKeyClass(Text.class);
		jobYearsAvg.setOutputValueClass(DoubleWritable.class);

		jobYearsAvg.setInputFormatClass(SequenceFileInputFormat.class);
		jobYearsAvg.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(jobYearsAvg, new Path(fileOutTemp));
		FileOutputFormat.setOutputPath(jobYearsAvg, new Path(fileOutTemp2));

		// Sorting job (by tempo descending)
		Job jobSort = Job.getInstance(conf, "Average Tempo - sort job");
		jobSort.setJarByClass(TempoAverage.class);
		jobSort.setMapperClass(SortMapper.class);
		jobSort.setReducerClass(SortReducer.class);
		jobSort.setOutputKeyClass(DoubleWritable.class);
		jobSort.setOutputValueClass(Text.class);

		// read from the sequence file produced by the first job
		jobSort.setInputFormatClass(SequenceFileInputFormat.class);
		jobSort.setSortComparatorClass(LongInverseComparator.class);

		FileInputFormat.addInputPath(jobSort, new Path(fileOutTemp2));
		FileOutputFormat.setOutputPath(jobSort, new Path(fileOut));

		// Job start
		JobControl jobControl = new JobControl("average-tempo-control") {
			{
				ControlledJob count = new ControlledJob(jobTempoMedian, null);
				ControlledJob avg = new ControlledJob(jobYearsAvg, Collections.singletonList(count));
				ControlledJob sort = new ControlledJob(jobSort, Collections.singletonList(avg));
				addJob(count);
				addJob(avg);
				addJob(sort);
			}
		};
		(new Thread(jobControl)).start();

		int seconds = 0;
		while (!jobControl.allFinished()) {
			LOG.info("Running for " + seconds + " seconds.");
			Thread.sleep(HEALTH_CHECK_INTERVAL * 1000);
			seconds += HEALTH_CHECK_INTERVAL;
		}
		jobControl.stop();

		/*
		 * cleanup and exit
		 **********************************************************************/
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(new Path(fileOutTemp), true);
		hdfs.delete(new Path(fileOutTemp2), true);

		LOG.info("Job finished: " + jobControl.getState());
		System.exit(0);
	}

}
