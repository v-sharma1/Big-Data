import java.io.IOException;
import java.lang.ClassNotFoundException;
import java.lang.InterruptedException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Context output)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum = sum + val.get();
		}
		output.write(key, new IntWritable(sum));
	}
}

class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable inputkey, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString(); // converting text to string
		String[] splits = line.split("\\W+"); // splitting words delimited by whitespace
		for (String outputkey : splits) {
			context.write(new Text(outputkey), new IntWritable(1));
		}
	}
}

public class WordCountDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration(); // tells where the namenode & resource manager resides
		Job job = new Job(conf, "WordCountDriver");
		job.setJarByClass(WordCountDriver.class);
		job.setMapperClass(WordCountMapper.class); // specifying the mapper class
		job.setReducerClass(WordCountReducer.class); // specifying the reducer class
		job.setNumReduceTasks(1); // specifying the no. of reduce tasks
		job.setInputFormatClass(TextInputFormat.class); // default input format is TextInputFormat
		job.setOutputKeyClass(Text.class);// specifying the reducers output key
		job.setOutputValueClass(IntWritable.class); // specifying the reducers output value
		FileInputFormat.addInputPath(job,
				new Path("hdfs://ip-172-31-35-141.ec2.internal:8020/user/vaibhavsharma806060/hadoop-wc/input.txt"));
		FileOutputFormat.setOutputPath(job,
				new Path("hdfs://ip-172-31-35-141.ec2.internal:8020/user/vaibhavsharma806060/hadoop-wc/result"));
		job.waitForCompletion(true);
	}
}
