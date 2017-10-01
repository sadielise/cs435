import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Reducer6 extends Reducer<Text,Text,Text,Text>{

	/*
	 * Input: (word, author|TF) or (word, IDF)
	 * Output: author|word|TF.IDF
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
		Map<String,Double> author_TF = new TreeMap<String,Double>();

		for(Text val: values){
			String[] columns = val.toString().split("\\|");
			if(columns.length == 1){
				IDF = Double.valueOf(columns[0]);
			}
			else {
				author_TF.put(columns[0], Double.valueOf(columns[1]));
			}
		}

		for(String author: author_TF.keySet()){
			Double TFIDF = author_TF.get(author) * IDF;
			output.write("job6output", new Text(author + "|" + key + "|" + TFIDF.toString()), new Text(""));
		}
	}
}