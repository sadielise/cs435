import java.io.IOException;
import java.util.TreeMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1A extends Reducer<Text,Text,Text,Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws
	IOException, InterruptedException {

		Map<String,Integer> years = new TreeMap<String,Integer>();
			
		for(Text year: values){
			String yearString = year.toString();
			if(years.containsKey(yearString)){
				years.put(yearString, years.get(yearString) + 1);
			}
			else{
				years.put(yearString, 1);
			}
		}
		
		for(String yearMapped: years.keySet()){
			context.write(key, new Text("\t" + yearMapped + "\t" + years.get(yearMapped)));
		}
	}
}