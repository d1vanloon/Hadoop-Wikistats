package cs5621.hadoop.wikistats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
	 * Name of temporary output directory for Job 1.
	 */
	private static final String JOB1_OUTPUT_TEMP = "/job1";
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
	 * Job 2 output directory.
	 */
	private static final String JOB_2_SUBFOLDER = "/job2";
	/**
	 * Job 3 output directory.
	 */
	private static final String JOB_3_SUBFOLDER = "/job3";

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
		Path job1OutputPath = new Path(args[OUTPUT_ARGS_INDEX] + JOB1_OUTPUT_TEMP);
		Path job2InputPath = job1OutputPath;
		Path job2OutputPath = new Path(args[OUTPUT_ARGS_INDEX] + JOB_2_SUBFOLDER);
		Path job3InputPath = job1OutputPath;
		Path job3OutputPath = new Path(args[OUTPUT_ARGS_INDEX] + JOB_3_SUBFOLDER);

		// Configuration set up

		Configuration conf1 = new Configuration();
		Configuration conf2 = new Configuration();
		Configuration conf3 = new Configuration();

		conf1.set(PAGES_PARAM_NAME, pages);
		conf1.set(LANGUAGES_PARAM_NAME, languages);
		conf1.set(PERIOD_PARAM_NAME, period);
		conf2.set(PAGES_PARAM_NAME, pages);
		conf2.set(LANGUAGES_PARAM_NAME, languages);
		conf2.set(PERIOD_PARAM_NAME, period);
		conf3.set(PAGES_PARAM_NAME, pages);
		conf3.set(LANGUAGES_PARAM_NAME, languages);
		conf3.set(PERIOD_PARAM_NAME, period);

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
		FileInputFormat.addInputPath(job1, job1InputPath);
		FileOutputFormat.setOutputPath(job1, job1OutputPath);

		// Job 2 set up
		
		/*
		 * Job 2 finds the top N pages for each language.
		 */

		Job job2 = new Job(conf2, "job2");
		job2.setJarByClass(WikiStats.class);
		job2.setMapperClass(WikiStatsJob2.Job2Mapper.class);
		job2.setReducerClass(WikiStatsJob2.Job2Reducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, job2InputPath);
		FileOutputFormat.setOutputPath(job2, job2OutputPath);
		
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

		// Controlled job set up, dependency for jobs 2, 3

		ControlledJob cJob1 = new ControlledJob(job1.getConfiguration());
		ControlledJob cJob2 = new ControlledJob(job3.getConfiguration());
		ControlledJob cJob3 = new ControlledJob(job3.getConfiguration());
		cJob1.setJob(job1);
		cJob2.setJob(job3);
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
		
		
		/*
		 * POST-PROCESSING GOES HERE
		 */
		
		// TODO: Process output of jobs 2, 3

		System.exit(0);
	}
}
