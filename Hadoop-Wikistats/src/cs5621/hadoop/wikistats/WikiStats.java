package cs5621.hadoop.wikistats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Map.Entry;

import java.net.URI;

/**
 * Job Control class and main
 * @author David Van Loon, Stephen Bernard
 */

public class WikiStats {
	/**
	 * Parameter name for accessing the parameter DayPeriod.
	 */
	static final String PERIOD_PARAM_NAME = "DayPeriod";
	/**
	 * Parameter name for accessing the parameter NumberOfLanguages.
	 */
	static final String LANGUAGES_PARAM_NAME = "NumberOfLanguages";
	/**
	 * Parameter name for accessing the parameter NumberOfPages.
	 */
	static final String PAGES_PARAM_NAME = "NumberOfPages";
	/**
	 * Argument index of the input path.
	 */
	private static final int INPUT_ARGS_INDEX = 0;
	/**
	 * Argument index of the output path.
	 */
	private static final int OUTPUT_ARGS_INDEX = 1;
	/**
	 * Argument index of the NumberOfPages argument.
	 */
	private static final int PAGES_ARGS_INDEX = 2;
	/**
	 * Argument index of the NumberOfLanguages argument.
	 */
	private static final int LANGUAGES_ARGS_INDEX = 3;
	/**
	 * Argument index of the DayPeriod argument.
	 */
	private static final int PERIOD_ARGS_INDEX = 4;
	/**
	 * Job 1 output directory.
	 */
	private static final String JOB_1_SUBFOLDER = "/job1";
	/**
	 * Job 2 output directory.
	 */
	private static final String JOB_2_SUBFOLDER = "/job2";
	/**
	 * Job 3 output directory.
	 */
	private static final String JOB_3_SUBFOLDER = "/job3";
	/**
	 * Job 4 output directory.
	 */
	private static final String JOB_4_SUBFOLDER = "/job4";

	public static void main(String[] args) throws Exception {

		if(args.length != 5){
			System.err.println("Usage: WikiStats <in> <out> <pages> <languages> <period>");
			System.exit(-1);
		}
		
		String pages = args[PAGES_ARGS_INDEX];
		String languages = args[LANGUAGES_ARGS_INDEX];
		String period = args[PERIOD_ARGS_INDEX];

		// Path set up

		Path job1InputPath = new Path(args[INPUT_ARGS_INDEX]);
		Path job1OutputPath = new Path(args[OUTPUT_ARGS_INDEX] + JOB_1_SUBFOLDER);
		Path job2InputPath = job1OutputPath;
		Path job2OutputPath = new Path(args[OUTPUT_ARGS_INDEX] + JOB_2_SUBFOLDER);
		Path job3InputPath = job1OutputPath;
		Path job3OutputPath = new Path(args[OUTPUT_ARGS_INDEX] + JOB_3_SUBFOLDER);
		Path job4OutputPath = new Path(args[OUTPUT_ARGS_INDEX] + JOB_4_SUBFOLDER);

		// Configuration set up

		Configuration conf1 = new Configuration();
		Configuration conf2 = new Configuration();
		Configuration conf3 = new Configuration();
		Configuration conf4 = new Configuration();

		conf1.set("mapred.textoutputformat.separator", ";");
		conf1.set(PAGES_PARAM_NAME, pages);
		conf1.set(LANGUAGES_PARAM_NAME, languages);
		conf1.set(PERIOD_PARAM_NAME, period);
		conf2.set("mapred.textoutputformat.separator", ";");
		conf2.set(PAGES_PARAM_NAME, pages);
		conf2.set(LANGUAGES_PARAM_NAME, languages);
		conf2.set(PERIOD_PARAM_NAME, period);
		conf3.set("mapred.textoutputformat.separator", ";");
		conf3.set(PAGES_PARAM_NAME, pages);
		conf3.set(LANGUAGES_PARAM_NAME, languages);
		conf3.set(PERIOD_PARAM_NAME, period);
		conf4.set("mapred.textoutputformat.separator", ";");
		conf4.set(PAGES_PARAM_NAME, pages);
		conf4.set(LANGUAGES_PARAM_NAME, languages);
		conf4.set(PERIOD_PARAM_NAME, period);

		// Job 1 set up

		/*
		 * Job 1 calculates the spike over O date range.
		 */
		
		Job job1 = new Job(conf1, "job1");
		job1.setJarByClass(WikiStats.class);
		job1.setMapperClass(WikiStatsJob1.Job1Mapper.class);
		job1.setReducerClass(WikiStatsJob1.Job1Reducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setNumReduceTasks(64);
		FileInputFormat.addInputPath(job1, job1InputPath);
		FileOutputFormat.setOutputPath(job1, job1OutputPath);
		job1.waitForCompletion(true);
		
		// Job 2 set up
		
		/*
		 * Job 2 finds the top N pages for each language.
		 */
		Job job2 = new Job(conf2, "job2");
		job2.setJarByClass(WikiStats.class);
		job2.setMapperClass(WikiStatsJob2.Job2Mapper.class);
		job2.setReducerClass(WikiStatsJob2.Job2Reducer.class);
		job2.setPartitionerClass(WikiStatsJob2.SortSpikePartitioner.class);
		job2.setGroupingComparatorClass(WikiStatsJob2.GroupingComparator.class);
		job2.setSortComparatorClass(WikiStatsJob2.SortComparator.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setNumReduceTasks(64);
		FileInputFormat.addInputPath(job2, job2InputPath);
		FileOutputFormat.setOutputPath(job2, job2OutputPath);
		job2.waitForCompletion(true);
		
		// Job 3 set up
		
		/*
		 * Job 3 finds the top M languages.
		 */
		Job job3 = new Job(conf3, "job3");
		job3.setJarByClass(WikiStats.class);
		job3.setMapperClass(WikiStatsJob3.Job3Mapper.class);
		job3.setReducerClass(WikiStatsJob3.Job3Reducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job3, job3InputPath);
		FileOutputFormat.setOutputPath(job3, job3OutputPath);
		job3.waitForCompletion(true);
			
		// Job 4 set up
		
		/*
		 * Job 4 process outputs of Job 2 and Job 3 to generate final output.
		 */
		Job job4 = new Job(conf4, "job4");
		job4.setJarByClass(WikiStats.class);
		job4.setMapperClass(WikiStatsJob4.Job4Mapper.class);
		job4.setReducerClass(WikiStatsJob4.Job4Reducer.class);
		job4.setMapOutputKeyClass(IntWritable.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job4, job3OutputPath);
		FileInputFormat.addInputPath(job4, job2OutputPath);
		FileOutputFormat.setOutputPath(job4, job4OutputPath);
		job4.waitForCompletion(true);
		
			
		// Controlled job set up, dependency for jobs 2, 3
		/*
		ControlledJob cJob1 = new ControlledJob(job1.getConfiguration());
		ControlledJob cJob2 = new ControlledJob(job2.getConfiguration());
		ControlledJob cJob3 = new ControlledJob(job3.getConfiguration());
		cJob1.setJob(job1);
		cJob2.setJob(job2);
		cJob3.setJob(job3);
		cJob2.addDependingJob(cJob1);
		cJob3.addDependingJob(cJob1);

		// Job Control set up

		JobControl jobControl = new JobControl("WikiStats");
		jobControl.addJob(cJob1);
		jobControl.addJob(cJob2);
		jobControl.addJob(cJob3);

		// run the job control in a thread

		Thread t = new Thread(jobControl);
		t.start();

		// Wait until all jobs are complete
		while(!jobControl.allFinished())
			Thread.sleep(5000);
		
		*/

		System.exit(0);
	}

}
