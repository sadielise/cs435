import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

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
		job1.setJobName("Clean up data (from CSV file)");
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("/Job1"));
		LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
		
		MultipleOutputs.addNamedOutput(job1, "job1output", TextOutputFormat.class, NullWritable.class, Text.class);
		
		job1.waitForCompletion(true);
		
		
		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(MainClass.class);
		job2.setJobName("Calculate score for each university (including distributed cache file containing desired state, field, years, gender, family income)");
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job2, new Path("/Job1/job1output-r-00000"));
		FileOutputFormat.setOutputPath(job2, new Path("/Job2"));
		LazyOutputFormat.setOutputFormatClass(job2, TextOutputFormat.class);
		
		MultipleOutputs.addNamedOutput(job2, "job2output", TextOutputFormat.class, NullWritable.class, Text.class);
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
