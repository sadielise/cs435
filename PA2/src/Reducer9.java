import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Reducer9 extends Reducer<Text,Text,Text,Text>{

	/*
	 * Input: (word, author|TF) and (word, $)
	 * Output: author|word|TF (complete list)
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

		Map<Text,Text> author_TF = new TreeMap<Text,Text>();
		Map<Text,Text> authorMap = new TreeMap<Text,Text>();

		for(Text val: values){

			String[] vals = val.toString().split("\\|");
			if(vals.length > 1){
				String author = vals[0].trim();
				String TF = vals[1].trim();
				author_TF.put(new Text(author), new Text(TF));
			}
		}

		FileReader authorsFile = new FileReader(new File("./authorsFile"));
		BufferedReader br = new BufferedReader(authorsFile);
		String line;
		while((line = br.readLine()) != null){
			String lineTrimmed = line.trim();
			String author = lineTrimmed.substring(1);
			authorMap.put(new Text(author), new Text("x"));
		}
		br.close();

		for(Text author: authorMap.keySet()){
			String author_word_TFIDF = "";
			if(author_TF.containsKey(author)){
				author_word_TFIDF = author + "|" + key + "|" + author_TF.get(author);
			}
			else{
				author_word_TFIDF = author + "|" + key + "|" + "0.5";
			}
			output.write("job9output", new Text(author_word_TFIDF), new Text(""));
		}
	}
}