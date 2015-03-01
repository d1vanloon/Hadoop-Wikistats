package cs5621.hadoop.wikistats;

import java.io.IOException;
import java.lang.InterruptedException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Mapper for Job 2.
 * @author Stephen Bernard, Yan Bai
 *
 */

public class WikiStatsJob2Mapper extends Mapper<Text, Text, Text, Text>{
    
    private Text word = new Text();


    //key format = "EnPageName"
    //value format = "1238" spike in text
    @Override
    public void map(Text key, Text value, Context context)
	throws IOException, InterruptedException{
	
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
