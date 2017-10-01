import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Reducer11 extends Reducer<Text,Text,Text,Text>{

	/*
	 * Input: (author, word|TF.IDF)
	 * Output: author|word|TF.IDF (sorted list)
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

		for(Text val: values){
			String[] author_word = key.toString().split("\\|");
			String author_word_TFIDF = author_word[0] + "|" + val.toString();
			output.write("job11output", new Text(author_word_TFIDF), new Text(""));
		}
	}
}