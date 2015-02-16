/**
 * 
 */
package cs5621.hadoop.wikistats;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for Job 1.
 * @author David Van Loon
 *
 */
public class WikiStatsJob1Mapper extends
		Mapper<LongWritable, Text, Text, Text> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.map(key, value, context);
	}

	
}
