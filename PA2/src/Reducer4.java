import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Reducer4 extends Reducer<Text,Text,Text,Text>{
	
	/*
	 * Input: (word, author)
	 * Output: word|wordAuthorCount
	 */
	
	
	private MultipleOutputs<Text, Text> output;
	
	@Override
	public void setup(Context context){
		output = new MultipleOutputs<Text, Text>(context);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		output.close();
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws
	IOException, InterruptedException {

		Map<String,Integer> authors = new TreeMap<String,Integer>();

		// put all authors into tree map (doesn't allow duplicates)
		for(Text val: values){
			authors.put(val.toString(), 0);
		}
		
		// pull out deduped authors
		int count = 0;
		for(String author: authors.keySet()){
			count++;
		}
		
		String countString = String.valueOf(count);
		
		output.write("job4output", new Text(key + "|" + countString), new Text(""));
	}
	
}