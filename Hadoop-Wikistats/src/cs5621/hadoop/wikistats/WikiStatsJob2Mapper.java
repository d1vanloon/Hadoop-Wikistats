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
 * Mapper for Job 2.
 * @author Stephen Bernard
 *
 */

public class WikiStatsJob2Mapper extends Mapper<Text, LongWritable, Text, Text>{

    private Text langSpike = new Text();
    private Text page = new Text();

    public void map(Text key, LongWritable value, Context context){
	String temp = key.get();
	String language = temp.substring(0, 2);
	String pageStr = temp.substring(3);
	String spike = Integer.toString(value.get());

	langSpike.set(language + "," + spike);

	page.set(pageStr);

	context.write(langSpike, page);
    }
}
