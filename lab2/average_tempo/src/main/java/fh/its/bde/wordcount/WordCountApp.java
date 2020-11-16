package fh.its.bde.wordcount;

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

public class WordCountApp {
	private static final Logger LOG = Logger.getLogger(WordCountApp.class);
	private static final int HEALTH_CHECK_INTERVAL = 5;

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] parsedArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		/*
		 * check and fetch command line parameters
		 **********************************************************************/
		if (parsedArgs.length != 2) {
			LOG.error("Usage: AverageTempo <in> <out>");
			System.exit(2);
		}
		String fileIn = parsedArgs[0];
		String fileOut = parsedArgs[1];
		String fileOutTemp = fileOut + "temp_map";
		String fileOutTemp2 = fileOut + "temp_sort";

		LOG.info("STARTED!");
		/*
		 * Build counting job
		 **********************************************************************/
		Job jobTempoMedian = Job.getInstance(conf, "Average Tempo - year median job");
		jobTempoMedian.setJarByClass(WordCountApp.class);
		jobTempoMedian.setMapperClass(YearMapper.class);
		jobTempoMedian.setReducerClass(YearReducer.class);

		// three worker nodes
		jobTempoMedian.setNumReduceTasks(3);

		jobTempoMedian.setOutputKeyClass(Text.class);
		jobTempoMedian.setOutputValueClass(LongWritable.class);

		// write intermediate result into a sequence file
		jobTempoMedian.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(jobTempoMedian, new Path(fileIn));
		FileOutputFormat.setOutputPath(jobTempoMedian, new Path(fileOutTemp));

		/*
		 * Build years job
		 **********************************************************************/
		Job jobYearsAvg = Job.getInstance(conf, "Average Tempo - years average job");
		jobYearsAvg.setJarByClass(WordCountApp.class);
		jobYearsAvg.setMapperClass(YearsPeriodMapper.class);
		jobYearsAvg.setReducerClass(YearsPeriodReducer.class);

		jobYearsAvg.setOutputKeyClass(Text.class);
		jobYearsAvg.setOutputValueClass(LongWritable.class);

		// write intermediate result into a sequence file
		jobYearsAvg.setInputFormatClass(SequenceFileInputFormat.class);
		jobYearsAvg.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(jobYearsAvg, new Path(fileOutTemp));
		FileOutputFormat.setOutputPath(jobYearsAvg, new Path(fileOutTemp2));

		/*
		 * Build sorting job
		 **********************************************************************/
		Job jobSort = Job.getInstance(conf, "Average Tempo - sort job");
		jobSort.setJarByClass(WordCountApp.class);
		jobSort.setMapperClass(SortMapper.class);
		jobSort.setReducerClass(SortReducer.class);
		jobSort.setOutputKeyClass(LongWritable.class);
		jobSort.setOutputValueClass(Text.class);

		// read from the sequence file produced by the first job
		jobSort.setInputFormatClass(SequenceFileInputFormat.class);
		jobSort.setSortComparatorClass(LongInverseComparator.class);

		FileInputFormat.addInputPath(jobSort, new Path(fileOutTemp2));
		FileOutputFormat.setOutputPath(jobSort, new Path(fileOut));

		/*
		 * start jobs using a job control
		 **********************************************************************/
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

		// it is necessary to wait for the jobControl to set "allFinished"
		// !! Caution: the JobControl implementation does not end the thread
		// when the jobs are done => join() cannot be used
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
