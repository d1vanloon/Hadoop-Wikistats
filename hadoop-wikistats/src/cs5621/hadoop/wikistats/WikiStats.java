package cs5621.hadoop.wikistats;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
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
	public static void main(String[] args) throws Exception {

		if(args.length != 5){
			System.err.println("Usage: WikiStats <in> <out> <pages> <languages> <period>");
			System.exit(-1);
		}

		// Path set up

		Path job1InputPath = new Path(args[0]);
		Path job1OutputPath = new Path("job1-output-temp");
		Path job2InputPath = job1OutputPath
		Path job2OutputPath = new Path(args[1]);

		// Configuration set up

		Configuration conf1 = new Configuration();
		Configuration conf2 = new Configuration();

		conf1.set("NumberOfPages", args[2]);
		conf1.set("NumberOfLanguages", args[3]);
		conf1.set("DayPeriod", args[4]);
		conf2.set("NumberOfPages", args[2]);
		conf2.set("NumberOfLanguages", args[3]);
		conf2.set("DayPeriod", args[4]);

		// Job 1 set up

		Job job1 = new Job(conf1, "job1");
		job1.setJarByClass(WikiStats.class);
		job1.setMapperClass(Job1Mapper.class);
		job1.setReducerClass(Job1Reducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, job1InputPath);
		FileOutputFormat.setOutputPath(job1, job1OutputPath);

		// Job 2 set up

		Job job2 = new Job(conf2, "job2");
		job2.setJarByClass(WikiStats.class);
		job2.setMapperClass(Job2Mapper.class);
		job2.setReducerClass(Job2Reducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, job2InputPath);
		FileOutputFormat.setOutputPath(job2, job2OutputPath);

		// Controlled job set up, dependency for job 2

		ControlledJob cJob1 = new ControlledJob(job1.getConfiguration());
		ControlledJob cJob2 = new ControlledJob(job2.getConfiguration());
		cJob1.setJob(job1);
		cJob2.setJob(job2);
		cJob2.addDependingJob(job1);

		// Job Control set up

		JobControl jobControl = new JobControl();
		jobControl.addJob(job1);
		jobControl.addJob(job2);

		// run the job control in a thread

		Thread t = new Thread(jobControl);
		t.start();

		while(!jobControl.allFinished())
			Thread.sleep(5000);

		System.exit(0);
	}
}