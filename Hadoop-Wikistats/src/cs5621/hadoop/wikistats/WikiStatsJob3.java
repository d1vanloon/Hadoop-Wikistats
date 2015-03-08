package cs5621.hadoop.wikistats;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WikiStatsJob3{

	public static class Job3Mapper extends Mapper<Object, Text, Text, IntWritable>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
						          String line = value.toString();
											          String lang = line.substring(0,2);
																          context.write( new Text(lang), new IntWritable(1) );
		}
	
	}
	
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
