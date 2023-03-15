package part3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.log4j.Logger;

import part3.RelativeFreqStripe.HashMapWritable;

public class RelativeFreqStripeInMapper {
	//private static Logger logger = Logger.getLogger(Mapper.class);
	//private static Logger loggerReduce = Logger.getLogger(Reducer.class);
	
	
	 public static class Map extends Mapper<LongWritable, Text, Text, HashMapWritable> {	
		 private static HashMap<String, HashMapWritable> map;
		 
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			map = new HashMap<>();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try (Scanner scanner = new Scanner(value.toString())) {
				while (scanner.hasNextLine()) {
					String line = scanner.nextLine();
					String[] tokens = line.split("\\s+");
					int num = tokens.length;

					for (int i = 0; i < num; i++) {
						
						if(!map.containsKey(tokens[i])) map.put(tokens[i], new HashMapWritable());

						HashMapWritable submap = map.get(tokens[i]);

						for (int j = i + 1; j < num; j++) {
							if (tokens[j].equals(tokens[i])) break;
							submap.add(new Text(tokens[j]), 1);
						}
					}
				}
				
			}catch(Exception e) {
				System.out.println(e.toString());
			}
	    }
		
		@Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	      super.cleanup(context);
	      // Emit intermediate key-value pairs
	      for (Entry<String, HashMapWritable> entry : map.entrySet()) {
	        context.write(new Text(entry.getKey()), entry.getValue());
	      }
	    }
	 } 
	 
	
	 public static class Reduce extends Reducer<Text, HashMapWritable, Text, HashMapWritable> {
	
	    public void reduce(Text key, Iterable<HashMapWritable> values, Context context) 
	      throws IOException, InterruptedException {

	    	//loggerReduce.info(key.toString());
	    	HashMapWritable h = new HashMapWritable();
		 	for (HashMapWritable val : values) {
		 		//loggerReduce.info(val.toString());
		 	    h.add(val);
		 	}
		 	context.write(key, h);
	    }
	 }
	 	
	 
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Relative Frequency Pair Approach in-mapper combining");
		job.setJarByClass(RelativeFreqStripe.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(HashMapWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(HashMapWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean success = job.waitForCompletion(true);
		if(success)	System.out.println("Job completed successfully.");
		else System.out.println("Job failed.");
	    
	 }

}
