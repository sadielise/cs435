import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper12 extends Mapper<LongWritable, Text, Text, Text>{

	/*
	 * Input: author|word|TF.IDF
	 * Output: (word, author@TF.IDF)
	 */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		String[] columns = value.toString().split("\\|");
		context.write(new Text(columns[1]), new Text(columns[0] + "@" + columns[2]));
	}
}
