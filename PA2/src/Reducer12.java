import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Reducer12 extends Reducer<Text,Text,Text,Text>{

	/*
	 * Input: (word, author|TF.IDF)
	 * Output: word$author@TFIDF|author@TFIDF|etc...
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

		String inverted_vector = key.toString() + "$";
		ArrayList<String> sortedArray = new ArrayList<String>();
		for(Text val: values){
			sortedArray.add(val.toString());
		}
		
		Collections.sort(sortedArray);
		
		for(String str: sortedArray){
			inverted_vector += str;
			inverted_vector += "|";
		}
		
		output.write("job12output", new Text(inverted_vector), new Text(""));
	}
}