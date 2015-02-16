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
	
	private static final int LANGUAGE_INDEX = 0;
	private static final int PAGE_NAME_INDEX = 1;
	private static final int VIEWS_INDEX = 2;

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		/*
		 * Data exists in the following format:
		 * 	domain page_title count_views total_response_size
		 * For example:
		 * 	en Main_Page 42 50043
		 * Other domains can exist, for example:
		 * 	de.d Freiheit 176 314159
		 * This comes from wiktionary.org and will not be considered.
		 * Only two letter language codes are to be considered.
		 */
		
		// Grab the current line as a String from the input value
		String line = value.toString();
		// Split the line into tokens by spaces
		String[] tokens = line.split("[ ]+");
		// We should have 4 tokens
		assert(tokens.length == 4);
		
	}

	
}
