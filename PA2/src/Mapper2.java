import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<LongWritable, Text, Text, Text>{

	/*
	 * Input: word|author|wordCount
	 * Output: (author, word|wordCount)
	 */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		//split line into author, word, and wordCount
		String[] columns = value.toString().split("\\|"); 
		String word = columns[0].trim();
		String author = columns[1].trim();
		String wordCount = columns[2].trim();
		
		context.write(new Text(author), new Text(word + "|" + wordCount));
	}
}
