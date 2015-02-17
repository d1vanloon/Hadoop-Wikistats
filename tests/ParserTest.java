import java.util.*;
public class ParserTest {

    public static void parse(String[] data) {
	List<String> days = new ArrayList<String>();
	List<Integer> values = new ArrayList<Integer>();

	for (String line : data){
	    String[] words = line.split(" ");
	    if (!days.contains(words[0])){
		days.add(words[0]);
		values.add(Integer.parseInt(words[2]));
	    } else {
		
		values.set(days.indexOf(words[0]), values.get(days.indexOf(words[0])) + Integer.parseInt(words[2]));//], days.indexOf(words[0]));
	    }
	    
	    /*	    for (String str : words){
		System.out.println(str);
		}*/
	}
	for (int i = 0; i < days.size(); i++){
	    System.out.println("Day:  " + days.get(i) + "        PageCounts: " + values.get(i));
	}
    }

    public static void main(String args[]){
	System.out.println("Begining test");
	String data[] = new String[9];
	data[0] = "20140601 00 14532";
	data[1] = "20140601 01 12345";
	data[2] = "20140601 02 14563";
	data[3] = "20140710 00 56321";
	data[4] = "20140710 01 32648";
	data[5] = "20141120 00 54321";
	data[6] = "20141120 01 47382";
	data[7] = "20140330 00 42311";
	data[8] = "20140422 00 13414";
	parse(data);
    }
}