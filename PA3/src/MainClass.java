import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainClass {

	public static void main(String[] args) throws IOException, ClassNotFoundException,
	InterruptedException {

		if (args.length != 2) {
			System.out.printf("Usage: <jar file> <input dir> <output dir>\n");
			System.exit(-1);
		}

		Configuration conf = new Configuration();

		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(MainClass.class);
		job1.setJobName("read and parse wiki data");

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setMapperClass(WikiMapper.class);
		job1.setReducerClass(WikiReducer.class);
		
		job1.setInputFormatClass(WikiHBaseInputFormat.class);

		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
		System.exit(job1.waitForCompletion(true) ? 0 : 1);

	}
}