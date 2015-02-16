/**
 * 
 */
package cs5621.hadoop.wikistats;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Mapper for Job 1.
 * @author David Van Loon
 *
 */
public class WikiStatsJob1Mapper extends
		Mapper<LongWritable, Text, Text, Text> {
	
	private static final String PAGECOUNTS = "pagecounts";
	private static final String INPUT_LINE_TOKEN_DELIMITER = "[ ]+";
	private static final int LINE_TOKENS_COUNT = 4;
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
		 * 
		 * The data is stored in files with the following name format:
		 * 	pagecounts-${YEAR}${MONTH}${DAY}-${HOUR}0000.gz
		 */
		
		// Grab the current line as a String from the input value
		String line = value.toString();
		// Split the line into tokens by spaces
		String[] tokens = line.split(INPUT_LINE_TOKEN_DELIMITER);
		// We should have 4 tokens
		assert(tokens.length == LINE_TOKENS_COUNT);
		// Get the file name of the input file
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		assert(fileName.startsWith(PAGECOUNTS));
		
		// Only process languages with two-letter codes
		if (tokens[LANGUAGE_INDEX].length() == 2) {
			context.write(
					new Text(
							String.format("%1$s %2$s", 
									tokens[LANGUAGE_INDEX], 
									tokens[PAGE_NAME_INDEX])), 
					new Text(String.format("%1$s %2$s %3$s", 
									tokens[VIEWS_INDEX])));
		}
	}

	
}
