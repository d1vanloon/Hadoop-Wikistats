package cs5621.hadoop.wikistats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.lang.NumberFormatException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
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


	/**
	 * Mapper of Job 2.
	 *
	 * Input key: Language+Pagename. For example: enComputer
	 * Input Value: Largest spike of this page.
	 * 
	 * Output Key: Language+Spike(Text)
	 * Output Value: PageName + ";" + Spike(Text)
	 *
	 * In map function, we format our keys and values as above to be run through a
	 * secondary sort.
	 */
	public static class Job2Mapper extends Mapper<LongWritable, Text, Text, Text>{

    	@Override
    	public void map(LongWritable key, Text value, Context context)
						throws IOException, InterruptedException {
	
		String line = key.toString();

		//Split up 

		String lang = line.substring(0, 2);
		String page = line.substring(2);
		String spike = value.toString();

		//Convert aboves strings to our desired keys and values
	
		Text outputKey = new Text(lang + spike);
		Text outputValue = new Text(page + ";" + spike);

		context.write(outputKey, outputValue);
    	}
	}

	/** 
	 * Partitioner class
	 * We include spike in key,but we need to partition data only by lang code in key.
	 */
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

		/**
		 * Overriding compare to group our pairs so all of one languauge is done in one reducer
		 */
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
	
		/**
		 * This will sort our keys by the spike we included.  Done before the reducer.
		 */
		@Override
		public int compare(WritableComparable a, WritableComparable b){
			Text aText = (Text)a;
			Text bText = (Text)b;
			String aString = aText.toString();
			String bString = bText.toString();
			
			String aLang = aString.substring(0,2);
			String bLang = bString.substring(0,2);

			int aSpike = 1;
			int bSpike = 1;
			
			try{
				// As we have formatted our keys, the spike will start on the third character
					aSpike = Integer.parseInt(aString.substring(2));
					bSpike = Integer.parseInt(bString.substring(2));
			}catch(NumberFormatException e){
					e.printStackTrace();	
			}
			if(!aLang.equals(bLang))
				return aLang.compareTo(bLang); 
			else
				// sort the spikes if in the same language
				return bSpike - aSpike;
		}
			
	}

	/**
	 * Reducer of Job 2
	 *
	 * Input Key: Language+Spike(Text)
	 * Input Value: PageName + ";" + Spike(Text)
	 *
	 * Output Key: Language+PageName
	 * Output Value: Largest Spike for the page
	 */
	public static class Job2Reducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
						   throws IOException, InterruptedException {

			int numberOfPages = Integer.parseInt(conf.get(WikiStats.PAGES_PARAM_NAME));
			//numberOfPages = 5;
			String lang = key.toString().substring(0,2);

			int i = 0;

			if(numberOfPages > 0){

				//In our for loop, go only across the numberOfPages on the top.
				//Format the pairs as we need for Job 3.
				for(Text val : values){
					String compositeValue = val.toString();
					int splitIndex = compositeValue.lastIndexOf(";") + 1;
					String spike = compositeValue.substring(splitIndex);
					String pageName = compositeValue.substring(0, splitIndex - 1);
					context.write(new Text(lang + pageName), new Text(spike));
					i++;

					if(i >= numberOfPages)
						break;
				}
			}
		}
	}
}
