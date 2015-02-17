/**
 * 
 */
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
 * Mapper for Job 1.
 * @author David Van Loon
 *
 */
public class WikiStatsJob1Mapper extends
		Mapper<LongWritable, Text, Text, Text> {
	
	private static final int FILE_NAME_TOKENS_COUNT = 3;
	private static final int LANGUAGE_CHAR_LENGTH = 2;
	private static final int HOUR_END_INDEX = 2;
	private static final int HOUR_BEGIN_INDEX = 0;
	private static final String FILE_NAME_TOKEN_DELIMITER = "[-]+";
	private static final String PAGECOUNTS = "pagecounts";
	private static final String INPUT_LINE_TOKEN_DELIMITER = "[ ]+";
	private static final int LINE_TOKENS_COUNT = 4;
	private static final int LANGUAGE_INDEX = 0;
	private static final int PAGE_NAME_INDEX = 1;
	private static final int VIEWS_INDEX = 2;
	private static final int DATE_INDEX = 1;
	private static final int HOUR_INDEX = 2;

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
		 * For example:
		 * 	pagecounts-20140601-000000.gz
		 */
		
		/*
		 * Parse the input line
		 */
		
		// Grab the current line as a String from the input value
		String line = value.toString();
		// Split the line into tokens by spaces
		String[] tokens = line.split(INPUT_LINE_TOKEN_DELIMITER);
		// We should have 4 tokens
		assert(tokens.length == LINE_TOKENS_COUNT);
		
		System.out.println("Input line: " + line);
		
		/*
		 * Parse the input file name
		 */
		
		// Get the file name of the input file
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		// The file name should begin with "pagecounts"
		assert(fileName.startsWith(PAGECOUNTS));
		String[] fileNameTokens = fileName.split(FILE_NAME_TOKEN_DELIMITER);
		// We should have 3 tokens
		assert(fileNameTokens.length == FILE_NAME_TOKENS_COUNT);
		// Remove extra data from the hour token
		fileNameTokens[HOUR_INDEX] = fileNameTokens[HOUR_INDEX].substring(HOUR_BEGIN_INDEX, HOUR_END_INDEX);
		
		System.out.println("File name: " + fileName);
		
		/*
		 * Send the data to the reducer
		 */
		
		// Only process languages with two-letter codes
		if (tokens[LANGUAGE_INDEX].length() == LANGUAGE_CHAR_LENGTH) {
			System.out.printf("Output data: (\"%s\", \"%s\")", String.format(
					"%1$s %2$s", tokens[LANGUAGE_INDEX],
					tokens[PAGE_NAME_INDEX]), String.format("%1$s %2$s %3$s",
					fileNameTokens[DATE_INDEX], fileNameTokens[HOUR_INDEX],
					tokens[VIEWS_INDEX]));
			context.write(
					new Text(
							String.format("%1$s %2$s", 
									tokens[LANGUAGE_INDEX], 
									tokens[PAGE_NAME_INDEX])), 
					new Text(String.format("%1$s %2$s %3$s", 
									fileNameTokens[DATE_INDEX], 
									fileNameTokens[HOUR_INDEX], 
									tokens[VIEWS_INDEX])));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
		      System.err.println("Usage: WikiStatsJob1Mapper <in> <out>");
		      System.exit(2);
		    }
		    Job job = new Job(conf, "word count");
		    job.setJarByClass(WikiStatsJob1Mapper.class);
		    job.setMapperClass(WikiStatsJob1Mapper.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
