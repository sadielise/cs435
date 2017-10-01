import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class WikiReducer extends Reducer<Text,Text,Text,Text>{
	
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

		for(Text val: values){
			String k = key.toString();
			String v = val.toString();
			String out = k + "," + v;
			context.write(new Text(out), new Text(""));
		}
		
	}
	
}