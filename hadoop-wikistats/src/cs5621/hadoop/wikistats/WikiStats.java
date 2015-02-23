/**
 * 
 */
package cs5621.hadoop.wikistats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import cs5621.hadoop.wikistats.job1.WikiStatsJob1Mapper;

/**
 * Runs analysis of WikiStats data.
 * 
 * @author David Van Loon
 */
public class WikiStats {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: WikiStats <in> <out>");
			System.exit(2);
		}
		/*
		 * Set up Job 1:
		 */
		Job job1 = new Job(conf, "wikistats job1");
		job1.setJarByClass(WikiStatsJob1Mapper.class);
		job1.setMapperClass(WikiStatsJob1Mapper.class);
		// job1.setReducerClass(WikiStatsJob1Reducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		// The output path will likely need to be changed to interface with job
		// 2.
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
}
