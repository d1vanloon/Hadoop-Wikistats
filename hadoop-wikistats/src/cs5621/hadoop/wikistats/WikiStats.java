/**
 * 
 */
package cs5621.hadoop.wikistats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
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
		
		/*
		 * Set up Job 1:
		 */

		Configuration job1Configuration = new Configuration();
		Job job1 = Job.getInstance(job1Configuration, "wikistats job1");
		job1.setJarByClass(WikiStatsJob1.class);
		job1.setMapperClass(WikiStatsJob1.Job1Mapper.class);
		job1.setReducerClass(WikiStatsJob1.Job1Reducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, job1InputPath);
		FileOutputFormat.setOutputPath(job1, job1OutputPath);
		ControlledJob job1Control = new ControlledJob(job1, null);
	}
}
