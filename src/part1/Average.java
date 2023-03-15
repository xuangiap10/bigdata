package part1;
	
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

 	
public class Average {
	public static boolean isValidIP(String ip) {
		String pattern = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
							"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
							"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
							"([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
		return ip.matches(pattern);
	}
	public static boolean isNumeric(String str) {
		String pattern = "^\\d+$";
		return str.matches(pattern);
	}
 	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		public void map(LongWritable key, Text value, Context context) 
						throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
		    // Get the first and last item from the line
		    String firstItem = tokenizer.nextToken();
		    String lastItem = "";
		    
			while (tokenizer.hasMoreTokens()) {
					lastItem = tokenizer.nextToken();
			}
		    if (isValidIP(firstItem) && isNumeric(lastItem)) {
		        context.write(new Text(firstItem), new IntWritable(Integer.parseInt(lastItem)));
		     }	    
		}
	} 
 	
	public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			double sum = 0; 
			int cnt = 0;
			for (IntWritable val : values) {
				sum += val.get();
				cnt++;
			}
			double result = sum/cnt;
			context.write(key, new DoubleWritable(result));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "averagecomputation" );
		job.setJarByClass(Average.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
 	
}
