import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WikiMapper extends Mapper<LongWritable, Text, Text, Text>{


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		String[] lines = value.toString().split("\n");
		String[] vals = lines[0].split(" ");
		String revisionId = vals[2];
		String otherFields = "";
		otherFields += vals[1];
		otherFields += ",";
		for(int i = 3; i < vals.length; i++){
			if(i == 4){
				String[] dateVals = vals[i].split("-");
				String year = dateVals[0];
				otherFields += year;
			}
			else{
				otherFields += vals[i];				
			}
			if(i != vals.length-1){
				otherFields += ",";
			}
		}
		context.write(new Text(revisionId), new Text(otherFields));
	}
}
