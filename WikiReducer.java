/**
 * WikiReducer.Java is the class that contains the reduce method for project 1 of CS5621
 * WikiReducer.Java is part of a Map Reduce Job and inherits from the Map Reduce Framework
 *
 *
 *
 *
 **/

package org.apache.hadoop.examples;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class WikiReducer
    /*extends Reducer<Text, IntWritable, Text, IntWritable>*/ {
    
    /**
     * arguments:
     *    key: The Language of a page and the title of the page
     *    values: the date and hour of the article along with the pagecount for the article
     *    context: The place in which to write the output
     *
     * Note for Values:
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
     **/
    public void reduce(Text key, Iterable<Text> values, Context context) {
	
	int[] days = new int[60];
	


	for (int i = 0; i < days.length; i++) {
	    System.out.println("day " + i);
	}
    }

    public static void main(String[] args) throws Exception {
	System.out.println("Begining test");
    }
}