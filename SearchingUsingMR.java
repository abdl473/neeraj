import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SearchingUsingMR
{
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		if (args.length != 3)
		{
			System.err.println("Usage: SearchingUsingMR <input path> <output path> <search word>");
			System.exit(2);
		}

		conf.set("search_word", args[2]);
		
		Job job = new Job(conf, "Searching Using MR");
		job.setJarByClass(SearchingUsingMR.class);
		job.setMapperClass(SearchingMapper.class);
		job.setReducerClass(SearchingReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

	public static class SearchingMapper extends Mapper<Object, Text, Text, IntWritable>
	{
		String search_word = "";
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			search_word = context.getConfiguration().get("search_word");
			StringTokenizer itr = new StringTokenizer(value.toString());
			String token;
			while (itr.hasMoreTokens())
			{
				token = itr.nextToken();
				if (search_word.equals(token))
				{
					word.set(token);
					context.write(word, one);
				}
			}
		}
	}

	public static class SearchingReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable val : values)
			{
				sum += val.get();
			}

			result.set(sum);
			context.write(key, result);

		}
	}

}
