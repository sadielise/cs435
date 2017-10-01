import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper14 extends Mapper<LongWritable, Text, Text, Text>{

	/*
	 * Input: word$author@TFIDF|author@TFIDF|etc...|unknown@TFIDF|
	 * Output: (unknown|author, TFIDF|TFIDF)
	 */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] word_vals = value.toString().split("\\$");
		context.getCounter(PROJECT_COUNTER.ARRAY_LENGTH).setValue(word_vals.length);
		String[] author_TFIDF = word_vals[1].split("\\|");
		String[] unknown = author_TFIDF[0].split("@");
		String unknownAuthor = unknown[0];
		String unknownTFIDF = unknown[1];
		

		for(int i = 1; i < author_TFIDF.length; i++){
			String[] known = author_TFIDF[i].split("@");
			String knownAuthor = known[0];
			String knownTFIDF = known[1];
			context.write(new Text(unknownAuthor + "|" + knownAuthor), new Text(unknownTFIDF + "|" + knownTFIDF));
		}
	}
}
