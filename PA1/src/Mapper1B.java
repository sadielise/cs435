import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1B extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		//split line into author, date, text
		String[] columns = value.toString().split("<===>"); 
		String fullAuthor = columns[0];
		String[] authorVals = fullAuthor.split(" ");
		String author = authorVals[authorVals.length-1].trim();
		String text = columns[2];

		//replace punctuation in text and split into unigrams
		text = text.replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();
		String[] words = text.split(" ");

		for(String word: words) {
			if((word.trim()).length() > 0)
				context.write(new Text(word), new Text(author));
		}
	}
}
