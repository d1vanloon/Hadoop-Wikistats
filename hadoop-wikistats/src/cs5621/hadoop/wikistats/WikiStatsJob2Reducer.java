package cs5621.hadoop.wikistats;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Reducer for Job 2.
 * @author Stephen Bernard
 *
 */

public class WikiStatsMap2Mapper extends Mapper<Text, Text, Text, LongWritable>{

    private LongWritable result = new LongWritable();

    @OVERRIDE
    public void reduce(Text key, Iterable<Text> values, Context context){
	
	int temp = 0;

	for(Text val : values){
	}

    }