import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper4 extends Mapper<LongWritable, Text, Text, Text>{
	
	/*
	 * Input: word|author|wordCount
	 * Output: (word, author)
	 */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		//split line into word, author, wordCount
		String[] columns = value.toString().split("\\|"); 
		String word = columns[0].trim();
		String author = columns[1].trim();
		
		context.write(new Text(word), new Text(author));
	}
}
