import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1A extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		//split line into author, date, text
		String[] columns = value.toString().split("<===>"); 
		String fullDate = columns[1];
		String[] dateVals = fullDate.split(" ");
		String year =dateVals[dateVals.length-1].trim();
		String text = columns[2];

		//replace punctuation in text and split into unigrams
		text = text.replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();
		String[] words = text.split(" ");

		for(String word: words) {
			if((word.trim()).length() > 0)
				context.write(new Text(word), new Text(year));
		}
	}
}
