import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper10 extends Mapper<LongWritable, Text, Text, Text>{

	/*
	 * Input: word|IDF and author|word|TF
	 * Output: (word, author|TF) and (word, IDF)
	 */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		String[] columns = value.toString().split("\\|");
		if(columns.length == 3){
			String word = columns[1].trim();
			String author = columns[0].trim();
			String TF = columns[2].trim();
			context.write(new Text(word), new Text(author + "|" + TF));
		}
		else if(columns.length == 2){
			String word = columns[0].trim();
			String IDF = columns[1].trim();
			context.write(new Text(word), new Text(IDF));
		}
	}
}
