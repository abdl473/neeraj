import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortingUsingMR
{
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		if (args.length != 2)
		{
			System.err.println("Usage: SortingUsingMR <input path> <output path> ");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(SortingUsingMR.class);
		job.setMapperClass(SortingMapper.class);
		job.setReducerClass(SortingReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

	public static class SortingMapper extends Mapper<Object, Text, Text, NullWritable>
	{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			context.write(value, NullWritable.get());
		}
	}

	public static class SortingReducer extends Reducer<Text, NullWritable, Text, NullWritable>
	{

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			context.write(key, NullWritable.get());
		}
	}

}
