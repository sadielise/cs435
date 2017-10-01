import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Reducer2 extends Reducer<Text,Text,Text,Text>{

	/*
	 * Input: (author, word|wordCount)
	 * Output: word|author|TF
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

		Map<String,Double> word_wordCount = new TreeMap<String,Double>();

		double maxFrequency = 0;

		// find max frequency
		for(Text val: values){
			String[] vals = val.toString().split("\\|");
			String word = vals[0].trim();
			String count = vals[1].trim();
			Double wordCount = Double.valueOf(count);
			word_wordCount.put(word, wordCount);
			if(wordCount > maxFrequency){
				maxFrequency = wordCount;
			}
		}

		// calculate TF for each word
		for(String wordMapped: word_wordCount.keySet()){
			Double tf = 0.5 + (0.5 * (word_wordCount.get(wordMapped)/maxFrequency));
			output.write("job2output", new Text(wordMapped + "|"), new Text(key + "|" + tf.toString()));
		}
	}
}