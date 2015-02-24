/**
 * 
 */
package cs5621.hadoop.wikistats;

import java.awt.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Runs analysis of WikiStats data.
 * 
 * @author David Van Loon
 */
public class WikiStats {
	public static void main(String[] args) throws Exception {
		// Verify proper command-line arguments.
		if (args.length != 2) {
			System.err.println("Usage: WikiStats <in> <out>");
			System.exit(2);
		}

		/*
		 * Input and output paths for jobs:
		 */

		// The input for job 1 comes from the first command-line argument.
		Path job1InputPath = new Path(args[0]);
		// The output for job 1 goes to a temporary directory for input into job
		// 2.
		Path job1OutputPath = new Path("job1-output-temp");
		// TODO: Confirm that temporary directory is properly configured.
		// The input for job 2 is the same as the output for job 1.
		Path job2InputPath = job1OutputPath;
		// The output for job 2 is the second command-line argument.
		Path job2OutputPath = new Path(args[1]);
		
		// Holds a list of configured jobs.
		JobControl jobController = new JobControl("wikistats");
		
		/*
		 * Set up Job 1:
		 */

		Configuration job1Configuration = new Configuration();
		Job job1 = Job.getInstance(job1Configuration, "job1");
		job1.setJarByClass(WikiStatsJob1.class);
		job1.setMapperClass(WikiStatsJob1.Job1Mapper.class);
		job1.setReducerClass(WikiStatsJob1.Job1Reducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, job1InputPath);
		FileOutputFormat.setOutputPath(job1, job1OutputPath);
		// Job 1 does not have any dependencies.
		ArrayList<ControlledJob> job1Dependencies = new ArrayList<ControlledJob>();
		ControlledJob job1Control = new ControlledJob(job1, job1Dependencies);
		// Add the configured job 1 to the job controller.
		jobController.addJob(job1Control);
		
		/*
		 * Set up Job 2:
		 */
		
		Configuration job2Configuration = new Configuration();
		Job job2 = Job.getInstance(job2Configuration, "job2");
		// TODO: Conform job 2 to the following naming standards and uncomment.
		//job2.setJarByClass(WikiStatsJob2.class);
		//job2.setMapperClass(WikiStatsJob2.Job2Mapper.class);
		//job2.setReducerClass(WikiStatsJob2.Job2Reducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setPartitionerClass(ActualKeyPartitioner.class);
		job2.setGroupingComparatorClass(ActualKeyGroupingComparator.class);
		job2.setSortComparatorClass(CompositeKeyComparator.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, job2InputPath);
		FileOutputFormat.setOutputPath(job2, job2OutputPath);
		// Job 2 is dependent on job 1.
		ArrayList<ControlledJob> job2Dependencies = new ArrayList<ControlledJob>();
		job2Dependencies.add(job1Control);
		ControlledJob job2Control = new ControlledJob(job2, job2Dependencies);
		// Add the configured job 2 to the job controller.
		jobController.addJob(job2Control);
		
		/*
		 * Run the configured jobs.
		 */
		
		jobController.run();
		
		// While jobs are still running...
		while (!jobController.allFinished()) {
			// Wait 5 seconds and check again.
			Thread.sleep(5000);
		}
	}
}
