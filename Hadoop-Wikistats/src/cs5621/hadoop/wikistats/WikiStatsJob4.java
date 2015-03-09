package cs5621.hadoop.wikistats;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 * Job class for Job 4.
 *
 * @author Yan Bai
 *
 * The goal of this job is to generate the final result based on outputs of job 2 and job 3.
 * Output of job 2 includes top N pages for each languages.
 * Output of job 3 includes number of unique page for each languages.
 * So in job 4, we need to do 2 things:
 * 	1. Based on output of job 3, rank languages based on number of unique paged and then pick top M languages. 
 *  2. After 1, we have top M languages. Then we read output of job 2, filter out all records rather than
 *     Top M languages.
 *
 * Then we get our final output.
 */

public class WikiStatsJob4{

	/**
	 * Mapper Class for Job 4.
	 *
	 *@author Yan Bai
	 *
	 * In the map function, we just emit the value to reducer. Key is a IntWritable 0.
	 * Actully, we will not process key in reducer and we just ignore it. All data emitted
	 * fro Mapper own same key so they will go into same reducer.
	 */ 
	public static class Job4Mapper extends Mapper<Object, Text, IntWritable, Text>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			context.write( new IntWritable(0), value );
		}
	
	}
	
	/**
	 * Reducer class for Job 4.
	 *
	 * @author Yan Bai
	 *
	 * All the output of job 2 and job 3 will come into one reducer since in mapper, the output is key is 0.
	 * That is what we want. Because we need all output data of job 2 and job 3 to generate final result.
	 *
	 * Procedure of reduce:
	 * 1.Based on the output of job 3, we will get top M most common languages.
	 * 2.Based on top M most common languages we just got and output of job 2, we will get top N pages of top M most common languages.
	 * 3.Write output. 
	 */ 
	public static class Job4Reducer extends Reducer<IntWritable, Text, Text, Text> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
											
			Configuration conf = context.getConfiguration();
			int M = Integer.parseInt(conf.get(WikiStats.LANGUAGES_PARAM_NAME));
			
			//This array is used for storing top M languages in descending order. Just code of languages.
			String[] topLangs = new String[M];					
			
			//Used for sorting all languages to find top M languages
			TreeMap<Integer, String> allLangs = new TreeMap<Integer, String>();					
			
			//HashMap to store top N pages for all languages. We will go through
			//this map and pick just records for top M languages.
			ArrayList<String> allPages = new ArrayList<String>();
			
			//Go through all input datas.
			//Push language data(output of job 3) to allLangs TreeMap.
			//Push page data(output of job 2) to allPages HashMap. 
			String str = null;
			String[] strs = null;
			for(Text value : values){
							str = value.toString();
							strs = str.split(";");
							if(strs.length == 2) {
									allLangs.put(Integer.parseInt(strs[1]), strs[0]);
							} else if(strs.length == 3) {
									allPages.add(value.toString());	
							}
			}

			/**
			 * In the TreeMap, key is the number of unique page, value is the code of language.
			 * And TreeMap sort data in ascending order, so we just need to pop datas on the head,
			 * and keey last M items. That will be top M languages.
			 */
			int numOfLangs = allLangs.size();
			for(int i=0; i<numOfLangs-M; i++){
					allLangs.pollFirstEntry();
			}

			//Push top M language codes to array for furthur use.
			int CopyOfM = M;
			for(Entry langEntry : allLangs.entrySet()){
					String langCode = (String)langEntry.getValue();
					topLangs[CopyOfM-1] = langCode;
					CopyOfM--;	
			}
			
			/**
			 * Create HashMap with size of M. Each item in HashMap is a linkedlist
			 * to store top N pages. 
			 */
			HashMap<String, LinkedList> pagesMap = new HashMap<String, LinkedList>();
			for(int j=0; j<topLangs.length; j++){
					pagesMap.put(topLangs[j], new LinkedList<String>());
			}
			
			// Push top N pages of each languages to HashMap.
			for(int k=0; k<allPages.size(); k++){
					String line = allPages.get(k);
					String[] strsInLine = line.split(";");
					String codeOfLang = strsInLine[0];
					String nameOfPage = strsInLine[1];
					String accessOfPage = strsInLine[2];
					if(pagesMap.containsKey(codeOfLang))pagesMap.get(codeOfLang).add(nameOfPage+" "+accessOfPage);
			}


			// write top N pages of top M languages to output
			for(int m=0; m<topLangs.length; m++){
					LinkedList pageList = pagesMap.get(topLangs[m]);
					while(!pageList.isEmpty()){
							context.write( new Text(topLangs[m]), new Text((String)pageList.poll()) );
					}
			}

			//write top M languages to output
			while(!allLangs.isEmpty()){
					Entry entryTopLang = allLangs.pollLastEntry();
					String accessOfTopLang = Integer.toString((Integer)entryTopLang.getKey());
					String codeOfTopLang = (String)entryTopLang.getValue();
					context.write(new Text(codeOfTopLang), new Text(accessOfTopLang));
			}
											
		
		}
	
	}

}
