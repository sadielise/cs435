import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Reducer8 extends Reducer<Text,Text,Text,Text>{

	/*
	 * Input: (x, word) and (x, $author)
	 * Output: word|authorList separated by #
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
		
		Map<String,String> authors = new TreeMap<String,String>();
		Map<String,String> words = new TreeMap<String,String>();
		
		for(Text val: values){
			String strVal = val.toString();
			if(strVal.charAt(0) == '$'){
				authors.put(strVal.substring(1), "x");
			}
			else{
				words.put(strVal, "x");
			}
		}
		
		for(String word: words.keySet()){
			String word_authors = word + "|";
			for(String author: authors.keySet()){
				word_authors += author;
				word_authors += "#";
			}
			output.write("job8output", new Text(word_authors), new Text(""));
		}
	}
}