import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 *
 * @author Stephen Bernard
 */

public class CompositeKey implements WritableComparable{

    private String language;
    private int spike;

    public CompositeKey(){
    }

    public CompositeKey(String language, int spike){

	this.language = language;
	this.spike = spike;

    }

    @Override
    public String toString(){
    
	return (new StringBuilder()).append(language).append(',').append(Integer.toString(spike)).toString();
    }

    @Override
	public void readFields(DataInput in) throws IOException{

	language = WritableUtils.readString(in);
	page = WritableUtils.readVInt(in);
    }

    @Override
	public void write(DataOutput out) throws IOException{

	WritableUtils.writeString(out, language);
	WritableUtils.writeVInt(out, spike);
    }

    @Override
	public int compareTo(CompositeKey o){

	return(spike.compareTo(o.spike));
    }

    public String getLanguage(){
	return langugage;
    }

    public int getSpike(){
	return spike;
    }

    public void setLanguage(String language){
	this.language = language;
    }

    public void setSpike(int spike){
	this.spike = spike;
    }

}

ActualKeyPartitioner extends Partitioner<CompositeKey, Text>{

    HashPartitioner<Text, Text> hashPartitioner = new HashPartitioner<Text, Text>();
    Text language = new Text();

@Override
    public int getPartition(CompositeKey key, Text value, int numReduceTasks){

    try{
	language.set(key.getLanguage());
	return hashPartitioner.getPartition(language, value, numReduceTasks);
    } catch (Exception e){
	e.printsStackTrace();
	return(int)(Math.random()*numReduceTasks);
    }
}
}