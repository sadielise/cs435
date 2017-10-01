import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper8 extends Mapper<LongWritable, Text, Text, Text>{

	/*
	 * Input: word and $author
	 * Output: (x, word) and (x, $author)
	 */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		context.write(new Text("x"), new Text(value));
	}
}
