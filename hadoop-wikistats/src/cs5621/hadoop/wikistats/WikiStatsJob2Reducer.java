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

    private Text langPage = new Text();

    @OVERRIDE
    public void reduce(Text key, Iterable<Text> values, Context context){
	
	private String temp = key.get();

	private String language = temp.substring(0,2);

	private int spike = Integer.parseInt(temp.substring(3));

	//private int topPages = foo;

	private int i = 0;

	for(Text val : values){
	    langPage.set(language + "," +  val);
	    result.set(spike);
	    context.write(langPage, result);

	    i++;
	    if(i >= topPages)
		break;
	}

    }