import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper3 extends Mapper<LongWritable, Text, Text, Text>{
	
	/*
	 * Input: word|author|wordCount
	 * Output: (x, author)
	 */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		//split line into word, author, wordCount
		String[] columns = value.toString().split("\\|"); 
		String author = columns[1].trim();
		
		context.write(new Text("x"), new Text(author));
	}
}
