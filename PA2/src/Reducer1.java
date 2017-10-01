import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Reducer1 extends Reducer<Text,Text,Text,Text>{
	
	/*
	 * Input: (word|author, x)
	 * Output: word|author|wordCount
	 */
	
	
	private MultipleOutputs<Text, Text> output;
	
	@Override
	public void setup(Context context){
		output = new MultipleOutputs<Text, Text>(context);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		output.close();
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws
	IOException, InterruptedException {

		int count = 0;
		
		for(Text val: values){
			count++;
		}
		
		String countString = String.valueOf(count);
		
		output.write("job1output", key, new Text("|"+ countString));
	}
	
}