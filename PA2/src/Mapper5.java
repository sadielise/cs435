import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper5 extends Mapper<LongWritable, Text, Text, Text>{
	
	/*
	 * Input: word|wordAuthorCount
	 * Output: (word, wordAuthorCount|totalAuthorCount)
	 */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		//split line into word, wordAuthorCount
		String[] columns = value.toString().split("\\|"); 
		String word = columns[0].trim();
		String wordAuthorCount = columns[1].trim();
		//Configuration conf = context.getConfiguration();
		String totalAuthorCount = "7";
		
		context.write(new Text(word), new Text(wordAuthorCount + "|" + totalAuthorCount));
	}
}
