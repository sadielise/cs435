import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text>{
	
	/*
	 * Input: author<===>date<===>text
	 * Output: (word|author, x)
	 */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		//split line into author, date, text
		String[] columns = value.toString().split("<===>"); 
		String authorFull = columns[0].trim();
		String[] authorPieces = authorFull.split(" ");
		String author = authorPieces[authorPieces.length-1].trim();
		String text = columns[2];

		//replace punctuation in text and split into unigrams
		text = text.replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();
		String[] words = text.split(" ");

		for(String word: words) {
			Text word_author = new Text(word + "|" + author);
			if((word.trim()).length() > 0)
				context.write(word_author, new Text("x"));
		}
	}
}