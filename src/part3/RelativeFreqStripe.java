package part3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;


public class RelativeFreqStripe {

	private static Logger logger = Logger.getLogger(Mapper.class);
	private static Logger loggerReduce = Logger.getLogger(Reducer.class);
	
	public static class HashMapWritable extends MapWritable{
		private static final long serialVersionUID = -4393659023305805436L;
		HashMapWritable() {super();}
		
		public void add(HashMapWritable h){
			 for (Entry<Writable, Writable> entry : h.entrySet()) {
				 add(entry.getKey(),Integer.parseInt(entry.getValue().toString()));
			 }
		}
		private void add(Writable key, Integer value){
			if(containsKey(key)) put(key,new IntWritable(Integer.parseInt(get(key).toString()) + value));
			else put(key,new IntWritable(value));
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			super.readFields(in);
			
		}
		@Override
		public void write(DataOutput out) throws IOException {
			super.write(out);
		}
		
		@Override
		public String toString(){
			String str = "";
			for (Entry<Writable, Writable> entry : entrySet()){
				if(!str.isEmpty()) str += ", "; 
				str += "(" + entry.getKey() + ", " + entry.getValue() + ")";
			}
			return str;
		}
	}
	
	 public static class Map extends Mapper<LongWritable, Text, Text, HashMapWritable> {
				 	
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	try (Scanner scanner = new Scanner(value.toString())) {
				while (scanner.hasNextLine()) {
					String  line = scanner.nextLine();
					String[] tokens = line.split("\\s+");
					int num = tokens.length;
					
					for(int i = 0; i < num; i++){
			
						HashMapWritable map = new HashMapWritable();

						for(int j = i+1; j < num; j++){
							if(tokens[j].equals(tokens[i]))	break;
	
							map.add(new Text(tokens[j]),1);
						}
						
						if(map.size() > 0) {
							context.write(new Text(tokens[i]), map);
							logger.info(tokens[i] + "  " + map.toString());
						}
					}
				}
				
			}catch(Exception e) {
				System.out.println(e.toString());
			}
	    }
	 } 
	 
	
	 public static class Reduce extends Reducer<Text, HashMapWritable, Text, HashMapWritable> {
	
	    public void reduce(Text key, Iterable<HashMapWritable> values, Context context) 
	      throws IOException, InterruptedException {

	    	loggerReduce.info(key.toString());
	    	HashMapWritable h = new HashMapWritable();
		 	for (HashMapWritable val : values) {
		 		loggerReduce.info(val.toString());
		 	    h.add(val);
		 	}
		 	context.write(key, h);
	    }
	 }
	 	
	 
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Relative Frequency Pair Approach");
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
	 
	 /*
	 public static void main(String[] args) throws Exception {
		 String filePath = "input/relativeFreq/file0";
		 String[] input_splits = new String[1];
			try {
				byte[] bytes = Files.readAllBytes(Paths.get(filePath));
				input_splits[0] = new String(bytes);
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}	
			
			try (Scanner scanner = new Scanner(input_splits[0])) {
				while (scanner.hasNextLine()) {
					String  line = scanner.nextLine();
					String[] tokens = line.split("\\s+");
					int num = tokens.length;
					
					for(int i = 0; i < num; i++){
						if(tokens[i].isEmpty()) continue;

						HashMapWritable map = new HashMapWritable();

						for(int j = i+1; j < num; j++){
							if(tokens[j].equals(tokens[i]))	break;
							if(tokens[j].isEmpty()) continue;
							map.add(new Text(tokens[j]),1);
						}
						
						int a = 0;
						System.out.println(tokens[i] + "   " +  map.toString());
						//if(map.size() > 0) context.write(new Text(tokens[i]), map);
					}
				}
				
			}catch(Exception e) {
				System.out.println(e.toString());
			}
		 
		 return;
	 }*/
}
