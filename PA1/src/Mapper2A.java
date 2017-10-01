import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2A extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		//split line into author, date, text
		String[] columns = value.toString().split("<===>"); 
		String fullDate = columns[1];
		String[] dateVals = fullDate.split(" ");
		String year = dateVals[dateVals.length-1].trim();
		String text = columns[2];

		//replace punctuation in text and split into unigrams
		text = text.replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();
		String[] words = text.split(" ");
		ArrayList<String> trimmedWords = new ArrayList<String>();

		for(String word: words) {
			if((word.trim()).length() > 0)
				trimmedWords.add(word);
		}
		
		if(trimmedWords.size() == 1){
			String currWord = trimmedWords.get(0);
			context.write(new Text("_START_ " + currWord), new Text(year));
			context.write(new Text(currWord + " _END_"), new Text(year));
		}
		else if(trimmedWords.size() > 1){
			String currWord = trimmedWords.get(0);
			context.write(new Text("_START_ " + currWord), new Text(year));
			for(int i = 1; i < trimmedWords.size()-1; i++){
				String nextWord = trimmedWords.get(i);
				context.write(new Text(currWord + " " + nextWord), new Text(year));
				currWord = nextWord;
			}
			context.write(new Text(trimmedWords.get(trimmedWords.size()-1) + " _END_"), new Text(year));
		}
	}
}
