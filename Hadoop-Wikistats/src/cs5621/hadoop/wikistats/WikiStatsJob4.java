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

public class WikiStatsJob4{

	public static class Job4Mapper extends Mapper<Object, Text, IntWritable, Text>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
								context.write( new IntWritable(0), value );
		}
	
	}
	
	public static class Job4Reducer extends Reducer<IntWritable, Text, Text, Text> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
											
											Configuration conf = context.getConfiguration();
											int M = Integer.parseInt(conf.get(WikiStats.LANGUAGES_PARAM_NAME));
											
											//Store top M languages in descending order
											String[] topLangs = new String[M];					
											
											//Used for sorting all languages to find top M languages
											TreeMap<Integer, String> allLangs = new TreeMap<Integer, String>();					
											
											//HashMap to store top N pages for all languages. Then we will just pick N pages for top M languages from it
										  ArrayList<String> allPages = new ArrayList<String>();
											
											//Go through all datas.
											//Push language data to allLangs TreeMap.
											//Push page data to allPages HashMap.
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

											//Pop elements in the MapTree, just keep last M elements which represent Top M languages.
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

										  HashMap<String, LinkedList> pagesMap = new HashMap<String, LinkedList>();
											for(int j=0; j<topLangs.length; j++){
													pagesMap.put(topLangs[j], new LinkedList<String>());
											}

											for(int k=0; k<allPages.size(); k++){
													String line = allPages.get(k);
													String[] strsInLine = line.split(";");
													String codeOfLang = strsInLine[0];
													String nameOfPage = strsInLine[1];
													String accessOfPage = strsInLine[2];
													if(pagesMap.containsKey(codeOfLang))pagesMap.get(codeOfLang).add(nameOfPage+";"+accessOfPage);
											}


											//write top N pages to output
											for(int m=0; m<topLangs.length; m++){
													LinkedList pageList = pagesMap.get(topLangs[m]);
													//context.write( new Text(topLangs[k]), new Text( Integer.toString(pageList.size())) );
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
