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
import java.util.*;

public class WikiReducer
   extends Reducer<Text, IntWritable, Text, IntWritable> {

    List<Text> days;
    List<IntWritable> values;
    IntWritable greatestSpike;
    int indexOfDay1 = 0;
    int indexOfDay2 = 0;
    
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
	
	parserSet(values);
	setSpike(); 
	Text value = new Text(days.get(indexOfA).toString() + " " + 
			      days.get(indexOfB).toString() + " " + greatestSpike.toString());
	context.write(key, value);
    }

    /*
     * A private method to parse the data reduce expects to perform calculations on better
     */
    public static void parserSet(Text[] data) {
	List<Text> days = new ArrayList<Text>();
	List<IntWritable> values = new ArrayList<IntWritable>();

	for (Text line : data){
	    String[] words = line.toString().split(" ");
	    Text dayToAdd  = new Text(words[0]);
	    IntWritable intToAdd = new IntWritable(Integer.parseInt(words[2]));
	    
	    if (!days.contains(dayToAdd)){
		days.add(dayToAdd);
		values.add(intToAdd);
	    } else {
		values.set(days.indexOf(dayToAdd), values.get(days.indexOf(dayToAdd)) + 
			   intToAdd);
	    }
	}
	this.days = new ArrayList<Text>(days);
	this.values = new ArrayList<IntWritable>(values);
    }

    private static void setSpike(){
	IntWritable greatestSpikeA = new IntWritable(0);
        IntWritable greatestSpikeB = new IntWritable(0);
        IntWritable greatestSpikeMagnitude = new IntWritable(0);
        for (int i = 0; i < this.days.length; i++) {
            int lookBacks = 5;
            if (i <= lookBacks) {
                lookBacks = i;
            }
            IntWritable currentGreatestSpike = new IntWritable(0);
            IntWritable currentA = new IntWritable(0);
            IntWritable currentB = new IntWritable(0);
            for (int j = 0; j < lookBacks; j++) {
                if (this.values.get(i) - this.values.get(i - j) > currentGreatestSpike) {
                    currentGreatestSpike = this.values.get(i) - this.values.get(i - j);
                    currentA = new IntWritable(i - j);
                    currentB = new IntWritable(i);
                }
            }
            if (currentGreatestSpike > greatestSpikeMagnitude) {
                greatestSpikeMagnitude = currentGreatestSpike;
                greatestSpikeA = currentA;
                greatestSpikeB = currentB;
            }
        }
	this.greatestSpike = greatestSpikeMagnitude;
	this.indexOfDay1 = currentA.get();
	this.indexOfDay2 = currentB.get();
    }
    
    public static void main(String[] args) throws Exception {
	System.out.println("Begining test");
    }
}