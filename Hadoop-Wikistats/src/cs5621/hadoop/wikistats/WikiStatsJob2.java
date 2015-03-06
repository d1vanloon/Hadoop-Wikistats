package cs5621.hadoop.wikistats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * @author Stephen Bernard, Yan Bai
 *
 */

public class WikiStatsJob2 {

	public static class Job2Mapper extends Mapper<Text, Text, Text, Text>{
    
    	private Text word = new Text();


    	//key format = "EnPageName"
    	//value format = "1238" spike in text
    	@Override
    	public void map(Text key, Text value, Context context)
						throws IOException, InterruptedException {
	
		String line = key.toString();
		String spike = value.toString();

		//String length = strs.length + " ";
		String lang = line.substring(0, 2);
		String page = line.substring(2);

		//Convert IntWritable value to String
	
		Text outputKey = new Text(lang);
		Text outputValue = new Text(page + "," + spike);

		context.write(outputKey, outputValue);
    	}
	}

	//Partitioner class
	//We include spike in key,but we need to partition data only by lang code in key.
	public static class SortSpikePartitioner extends Partitioner<Text, Text>{

	@Override
       	public int getPartition(Text key, Text value, int numPartitions){
			return (key.toString().substring(0,2).hashCode()) % numPartitions;
		}

	}	

  	//Grouping class
	//This class controls which keys are grouped together for a single call to Reducer.reduce()
	public static class GroupingComparator extends WritableComparator{

		public GroupingComparator(){
				super(Text.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b){
			Text ta = (Text)a;
			Text tb = (Text)b;
			String sa = ta.toString();
			String sb = tb.toString();
        
			return sa.substring(0,2).compareTo(sb.substring(0,2));
		}

	}

	//Secondary sort class
	public static class SortComparator extends WritableComparator{

		public SortComparator(){
			super(Text.class, true);
		}
	
		@Override
		public int compare(WritableComparable a, WritableComparable b){
			Text aText = (Text)a;
			Text bText = (Text)b;
			String aString = aText.toString();
			String bString = bText.toString();
			
			String aLang = aString.substring(0,2);
			String bLang = bString.substring(0,2);
			int aSpike = Integer.parseInt(aString.substring(2));
			int bSpike = Integer.parseInt(bString.substring(2));
	
			if(!aLang.equals(bLang))
				return aLang.compareTo(bLang); 
			else
				return bSpike - aSpike;
		}
			
	}

	public static class Job2Reducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
						   throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			int numberOfPages = Integer.parseInt(conf.get(WikiStats.PAGES_PARAM_NAME));
			String language = key.toString();

			int i = 0;

			if(numberOfPages > 0){

				for(Text val : values){
					String temp = val.toString();
					int split = temp.lastIndexOf(",");
					String page = temp.substring(0, split);
					String spike = temp.substring(split + 1);

					Text outputKey = new Text(language + page);
					Text outputValue = new Text(spike);

					context.write(outputKey, outputValue);
					i++;

					if(i >= numberOfPages)
						break;
				}
			}
		}
	}
}