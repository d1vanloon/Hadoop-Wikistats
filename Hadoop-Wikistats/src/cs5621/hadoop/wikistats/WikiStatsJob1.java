package cs5621.hadoop.wikistats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Job class for Job 1.
 * 
 * @author David Van Loon
 *
 */
public class WikiStatsJob1 {

	/**
	 * Mapper class for Job 1.
	 * 
	 * @author David Van Loon
	 *
	 */
	public static class Job1Mapper extends
			Mapper<LongWritable, Text, Text, Text> {

		/**
		 * The number of tokens in the file name.
		 */
		private static final int FILE_NAME_TOKENS_COUNT = 3;
		/**
		 * The character length of a language designator.
		 */
		private static final int LANGUAGE_CHAR_LENGTH = 2;
		/**
		 * The end index of the hour.
		 */
		private static final int HOUR_END_INDEX = 2;
		/**
		 * The begin index of the hour.
		 */
		private static final int HOUR_BEGIN_INDEX = 0;
		/**
		 * The token delimiter of the file name.
		 */
		private static final String FILE_NAME_TOKEN_DELIMITER = "[-]+";
		/**
		 * The start of the file name.
		 */
		private static final String PAGECOUNTS = "pagecounts";
		/**
		 * The input line token delimiter.
		 */
		private static final String INPUT_LINE_TOKEN_DELIMITER = "[ ]+";
		/**
		 * The number of tokens in an input line.
		 */
		private static final int LINE_TOKENS_COUNT = 4;
		/**
		 * The index of the language.
		 */
		private static final int LANGUAGE_INDEX = 0;
		/**
		 * The index of the page name.
		 */
		private static final int PAGE_NAME_INDEX = 1;
		/**
		 * The index of the page views.
		 */
		private static final int VIEWS_INDEX = 2;
		/**
		 * The index of the date.
		 */
		private static final int DATE_INDEX = 1;
		/**
		 * The index of the hour.
		 */
		private static final int HOUR_INDEX = 2;

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
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

			String line = "";
			String fileName;

			try {

				/*
				 * Parse the input line
				 */

				// Grab the current line as a String from the input value
				line = value.toString();
				// Split the line into tokens by spaces
				String[] tokens = line.split(INPUT_LINE_TOKEN_DELIMITER);
				// We should have 4 tokens
				assert (tokens.length == LINE_TOKENS_COUNT);

				/*
				 * Parse the input file name
				 */

				// Get the file name of the input file
				fileName = ((FileSplit) context.getInputSplit()).getPath()
						.getName();
				// The file name should begin with "pagecounts"
				assert (fileName.startsWith(PAGECOUNTS));
				String[] fileNameTokens = fileName
						.split(FILE_NAME_TOKEN_DELIMITER);
				// We should have 3 tokens
				assert (fileNameTokens.length == FILE_NAME_TOKENS_COUNT);
				// Remove extra data from the hour token
				fileNameTokens[HOUR_INDEX] = fileNameTokens[HOUR_INDEX]
						.substring(HOUR_BEGIN_INDEX, HOUR_END_INDEX);

				/*
				 * Send the data to the reducer
				 */
			
				// Only process languages with two-letter codes
				if (tokens[LANGUAGE_INDEX].length() == LANGUAGE_CHAR_LENGTH) {
					// Write the output to the reduce function.
					/*
					 * Key: language + page 
					 * The language is a two-character string. 
					 * The page name is a string of characters. 
					 * The language and page are not separated by a space. 
					 * Example: "enMain_Page" 
					 * Value: date + hour + pageviews 
					 * The date is an 8-character string in the form YYYYMMDD. 
					 * The hour is a two-character string. 
					 * Pageviews is a string of characters. 
					 * The date, hour, and pageviews are separated by spaces. 
					 * Example: "20140601 00 156"
					 */
					context.write(
							new Text(String.format("%1$s%2$s",
									tokens[LANGUAGE_INDEX],
									tokens[PAGE_NAME_INDEX])),
							new Text(String.format("%1$s %2$s %3$s",
									fileNameTokens[DATE_INDEX],
									fileNameTokens[HOUR_INDEX],
									tokens[VIEWS_INDEX])));
				}
			} catch (Exception ex) {
				System.err.println("Exception encountered.");
				System.err.println("Input: " + line);
				System.err.println("Exception details: " + ex.toString());
			}
		}
	}

	/**
	 * Reducer class for Job 1.
	 * 
	 * @author Eric Christensen
	 *
	 */
	public static class Job1Reducer extends
			Reducer<Text, Text, Text, IntWritable> {

		private static final String PERIOD_PARAM_DEFAULT = "5";
		private List<Text> days;
		private List<IntWritable> values;
		private IntWritable greatestSpike;
	    /**
	     * arguments:
	     *    key: The Language of a page and the title of the page
	     *    oldValues: the date and hour of the article along with the pagecount for the article
	     *    context: The place in which to write the output
	     *
	     * Note for oldValues:
	     *    There will 24 hour datums for each day
	     *    There will be 60 days of data for each Key
	     *    So there will be 1440 total value datums in values
	     *    All of these will be on one line as a Text data type
	     *
	     * Goal:
	     *    Find the largest spike (increase in page views) for all 5 day intervals
	     *    of the Key
	     *
	     * Methodology:
	     *    1: Combine the 24 hour datums in to one "Day" datum
	     *    2: Start at day 1 of 60
	     *    3: Take the magnitude of the difference of the current day's
	     *           page views to each of previous 5 days (all days if there are less than 5)
	     *    4: Repeat until day 60 keeping track of the greatest magnitude
	     *
	     * Output:
	     *    Max spike, Key
	     * @throws InterruptedException 
	     * @throws IOException 
	     **/
		public void reduce(Text key, Iterable<Text> oldValues, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int daysParam = Integer.parseInt(conf.get(WikiStats.PERIOD_PARAM_NAME,
					PERIOD_PARAM_DEFAULT));

			parserSet(oldValues);
			setSpike(daysParam);
			IntWritable value = new IntWritable(greatestSpike.get());
			context.write(key, value);
		}


		/*
		 * A private method to parse the data reduce expects to perform
		 * calculations on better
		 */
		private void parserSet(Iterable<Text> data) {
		        // A List of Text to hold the local day variables
			List<Text> dummyDays = new ArrayList<Text>();
			// A List of IntWritables to hold the local value variables
			List<IntWritable> dummyValues = new ArrayList<IntWritable>();

			// For every line of data in the input values of reduce
			for (Text line : data) {
			        // Split the line up into an easily operatable array of strings
				String[] words = line.toString().split(" ");
				// The day of the line will be at the 0 index of the array
				Text dayToAdd = new Text(words[0]);
				// The page count value will be at the third index of the array
				IntWritable intToAdd = new IntWritable(
						Integer.parseInt(words[2]));
				// Add the day of the current line to the list of days if it is not already present
				//     Add the value to the list of values if this is the case
				// If the day is already present in the list of days then simply add the value of the
				//     current line to the same index that the current day is at in dummyValues
				if (!dummyDays.contains(dayToAdd)) {
					dummyDays.add(dayToAdd);
					dummyValues.add(intToAdd);
				} else {
					IntWritable valueToAdd = new IntWritable(dummyValues.get(
							dummyDays.indexOf(dayToAdd)).get()
							+ intToAdd.get());
					dummyValues.set(dummyDays.indexOf(dayToAdd), valueToAdd);
				}
			}
			// Put the local day data into reduce's global days list
			days = new ArrayList<Text>(dummyDays);
			// Put the local values data in to reduce's global values list
			values = new ArrayList<IntWritable>(dummyValues);
		}


	    private void setSpike(int dayPeriod){
		
		// The local variable for the greatest spike
		IntWritable greatestSpikeMagnitude = new IntWritable(0);
		// For every piece of data in a days worth of values
		for (int i = 0; i < days.size(); i++) {
		    // Initialize the number of days to look back and make comparisons to to five
		    int lookBacks = dayPeriod;
		    // If this iteration is less than the default set lookBacks to the value of the current iteration
		    if (i <= lookBacks) {
			lookBacks = i;
		    }
		    // The local value for the greatest spike that will be used in the inner for loop
		    IntWritable currentGreatestSpike = new IntWritable(0);

		    // Iterate through each of the values that are a "lookBack" distance from the current iteration
		    for (int j = 0; j < lookBacks; j++) {
			// If the pagecount at the current iteration of i minus the page count of the
			// iteration of i - j is greater than the currentGreatestSpike, then this new
			// value becomes the current greatest spike
			if (values.get(i).get() - values.get(i - j).get() > currentGreatestSpike.get()) {
			    currentGreatestSpike = new IntWritable(values.get(i).get() - values.get(i - j).get());
			}
		    }
		    // If the greatest spike from the inner for loop is greater than the current total greatest spike
		    // then the overall greatest spike is set accordingly
		    if (currentGreatestSpike.get() > greatestSpikeMagnitude.get()) {
			greatestSpikeMagnitude = currentGreatestSpike;
		    }
		}
		greatestSpike = greatestSpikeMagnitude;
	    }
	}

	/**
	 * Tests WikiStatsJob1 as a stand-alone job.
	 * 
	 * @param args
	 *            the command-line arguments
	 * @throws Exception
	 *             if an error is encountered
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: WikiStatsJob1 <in> <out> <days>");
			System.exit(2);
		}
		conf.set(WikiStats.PERIOD_PARAM_NAME, args[2]);
		Job job = new Job(conf, "wikistats");
		job.setJarByClass(WikiStatsJob1.class);
		job.setMapperClass(WikiStatsJob1.Job1Mapper.class);
		job.setReducerClass(WikiStatsJob1.Job1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
