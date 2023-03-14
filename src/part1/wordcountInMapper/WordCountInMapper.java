package part1.wordcountInMapper;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 	
public class WordCountInMapper {
 	
	 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	    private HashMap<String, Integer> map;
	    
	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	      super.setup(context);
	      map = new HashMap<>();
	    }
	 	
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	try (Scanner scanner = new Scanner(value.toString())) {
				while (scanner.hasNextLine()) {
					String  line = scanner.nextLine();
					String[] tokens = line.split("[ ,\"'!?/-]");
					for (String token : tokens) {
						if(!token.isEmpty() && token.charAt(token.length()-1) == '.') 
							token = token.substring(0,token.length()-1);
						if (!token.matches("[a-zA-Z]+"))	continue;
						
						String oneWord = token.toLowerCase();
						if(map.containsKey(oneWord))	map.put(oneWord,map.get(oneWord)+1);
				 		else map.put(oneWord, 1);
						
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
	      for (Entry<String, Integer> entry : map.entrySet()) {
	        context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
	      }
	    }
	 } 
	 	
	 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	      throws IOException, InterruptedException {
	        int sum = 0;
		 	for (IntWritable val : values) {
		 	    sum += val.get();
		 	}
		 	context.write(key, new IntWritable(sum));
	    }
	 }
	 	
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
		
	    Job job = Job.getInstance(conf, "wordcount in-mapper combining");
		job.setJarByClass(WordCountInMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
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

