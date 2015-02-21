package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SortSpike {

	//Mapper Class
  public static class SortSpikeMapper extends Mapper<Object, Text, Text, Text>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = value.toString();
      String[] strs = line.split(" ");
      
      //String length = strs.length + ""; 
      String lang = strs[0].substring(0,2);
      String page = strs[0].substring(2);
      String outputKey = lang + strs[1];
      String outputValue = page + "," + strs[1];
     
      if(lang.length() == 2){
     		context.write( new Text(outputKey), new Text(outputValue) );
      } 
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
	
				if(!aLang.equals(bLang)){
					return aLang.compareTo(bLang); 
				}else{
					return bSpike - aSpike;
				}	
		}
			
	}

	//Reducer Class
  public static class SortSpikeReducer extends Reducer<Text,Text,Text,Text> {
    
		private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
       
			  String keyStr = key.toString(); 
        MapWritable map = new MapWritable();
 				for(Text value : values){
						//if(!map.containsKey(value)) map.put(value, new BooleanWritable(true));
						context.write(new Text(keyStr.substring(0,2)), value);
				}
				
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(SortSpike.class);
    job.setMapperClass(SortSpikeMapper.class);
		job.setCombinerClass(SortSpikeReducer.class);
    job.setReducerClass(SortSpikeReducer.class);
		job.setPartitionerClass(SortSpikePartitioner.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
		job.setSortComparatorClass(SortComparator.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
