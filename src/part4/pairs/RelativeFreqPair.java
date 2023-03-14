package part4.pairs;


import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

 	
public class RelativeFreqPair {
 	
	
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    Configuration conf = context.getConfiguration();
		    long startTime = System.currentTimeMillis();
		    
			String line = value.toString();
		    String []tokens = line.split(" ");
		    for (int i = 0; i< tokens.length-1; i++){
		    	for(int j=i+1; j< tokens.length; j++){
		    		String item1 = tokens[i];
		    		String item2 = tokens[j];
		    		if (item1.equalsIgnoreCase(item2)) break;
		    		word.set(item1 + "," + item2);
		    		context.write(word, one);
		    	}
		    }
		    long mapTime = System.currentTimeMillis() - startTime;
		    context.getCounter("CustomCounters", "MapTime").increment(mapTime);
		    context.getCounter("CustomCounters", "MapMemoryUsage").increment(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
		}
		    
	} 
 	
	public static class Reduce extends Reducer<Text, IntWritable, Text, LongWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
		    long startTime = System.currentTimeMillis();
		    
			long sum = 0; 
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new LongWritable(sum));
			
			long reduceTime = System.currentTimeMillis() - startTime;
		    context.getCounter("CustomCounters", "ReduceTime").increment(reduceTime);
		    context.getCounter("CustomCounters", "ReduceMemoryUsage").increment(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "relativefreqpair" );
		job.setJarByClass(RelativeFreqPair.class);

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
