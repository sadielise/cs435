import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper13 extends Mapper<LongWritable, Text, Text, Text>{

	/*
	 * Input: word$author@TFIDF|author@TFIDF|etc... and word$unknown@TFIDF
	 * Output: (word, author@TFIDF|author@TFIDF|etc...) and (word, unknown@TFIDF)
	 */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		String[] columns = value.toString().split("\\$");
		context.write(new Text(columns[0]), new Text(columns[1]));
	}
}
