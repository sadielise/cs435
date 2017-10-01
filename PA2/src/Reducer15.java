import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Reducer15 extends Reducer<Text,Text,Text,Text>{

	/*
	 * Input: (x, author|cosineSimilarity)
	 * Output: top ten author|cosineSimilarity pairs
	 */

	private TreeMap<Double,Text> finalValues = new TreeMap<Double,Text>();
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

		for(Text value: values){
			String[] author_sim = value.toString().split("\\|");
			String author = author_sim[0].trim();
			String sim = author_sim[1].trim();
			Double simVal = Double.valueOf(sim);

			finalValues.put(simVal, new Text(author));

			if(finalValues.size() > 10){
				finalValues.remove(finalValues.firstKey());
			}
		}
		
		ArrayList<Double> finalSimilarities = new ArrayList<Double>();
		ArrayList<String> finalAuthors = new ArrayList<String>();
		for(Double d: finalValues.keySet()){
			finalSimilarities.add(d);
			finalAuthors.add(finalValues.get(d).toString());
		}
		
		for(int i = 9; i > -1; i--){
			output.write("job15output", new Text(finalAuthors.get(i) + "|" + finalSimilarities.get(i)), new Text(""));
		}
		
	}
}