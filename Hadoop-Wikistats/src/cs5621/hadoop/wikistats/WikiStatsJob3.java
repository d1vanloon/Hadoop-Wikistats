package cs5621.hadoop.wikistats;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Job class for Job 3.
 *
 * @author Yan Bai
 *
 * This goal of this job is to count number of unique pages for each languages.
 * 
 * Input: Output of Job 1.
 * Output: Number of unique pages for each languages.
 */
public class WikiStatsJob3{

	/*
	 * Mapper of Job 3.
	 *
	 * Input Key: Language+PageName. For example: enComputer
	 * Input Value: Largest spike of this page.
	 * 
	 * Output Key: Language(Text)
	 * Output Value: 1(IntWritable)
	 * 
	 * In map function, extract first two characters of key and take it as key of mapper output.
	 * The mapper output value is just an IntWritable. Actually it could be everything. 
	 */
	public static class Job3Mapper extends Mapper<Object, Text, Text, IntWritable>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				String line = value.toString();
				String lang = line.substring(0,2);
				
				context.write( new Text(lang), new IntWritable(1) );
		}
	
	}
	
	/*
	 *Reducer of Job 3.
	 *
	 * Input Key: Language(Text)
	 * Input Value: 1(IntWritable)
	 * 
	 * Output Key: Language(Text)
	 * OutputValue: Number of unique page(IntWritable)
	 *
	 * Mapper will output one record for each page of each language. So in reducer we just need to
	 * count the input then we will get number of unique page for each language.
	 */
	public static class Job3Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
				int counter = 0;
				for(IntWritable value : values){
						counter = counter + value.get();
				}
				
				context.write(key, new IntWritable(counter));
		}
	
	}

}
