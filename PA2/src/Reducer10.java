import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Reducer10 extends Reducer<Text,Text,Text,Text>{

	/*
	 * Input: (word, author|TF) and (word, IDF)
	 * Output: author|word|TF.IDF (complete list)
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
		
		Double IDF = new Double(0.0);
		TreeMap<String,Double> author_TF = new TreeMap<String,Double>();
		
		for(Text val: values){
			String[] splits = val.toString().split("\\|");
			if(splits.length == 1){
				IDF = Double.valueOf(splits[0]);
			}
			else if(splits.length == 2){
				author_TF.put(splits[0].trim(), Double.valueOf(splits[1]));
			}
		}
		
		for(String author: author_TF.keySet()){
			Double TFIDF = author_TF.get(author) * IDF;
			output.write("job10output", new Text(author + "|" + key + "|" + TFIDF.toString()), new Text(""));
		}
	}
}