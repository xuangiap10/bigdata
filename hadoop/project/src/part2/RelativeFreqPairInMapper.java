
package part2;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import part2.RelativeFreqPair.PairWritable;

 	
public class RelativeFreqPairInMapper {
 	
	
	
	public static class Map extends Mapper<LongWritable, Text, PairWritable, IntWritable> {
		
	 	private HashMap<PairWritable, Integer> pairMap;
	 	public void setup(Context context){
	 		pairMap = new HashMap<PairWritable, Integer>();
	 	}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
		    String []tokens = line.split(" ");
		    for (int i = 0; i< tokens.length-1; i++){
		    	for(int j=i+1; j< tokens.length; j++){
		    		String item1 = tokens[i];
		    		String item2 = tokens[j];
		    		if (item1.equalsIgnoreCase(item2)) break;
		    		//String keypair = item1 + "," + item2;
		    		PairWritable keypair = new PairWritable(item1, item2);
			 		if (pairMap.containsKey(keypair)){
			 			pairMap.put(keypair, pairMap.get(keypair)+ 1);
			 		} else {
			 			pairMap.put(keypair, 1);
			 		}
			 		PairWritable keypair1 = new PairWritable(item1, "*");
			 		if (pairMap.containsKey(keypair1)){
			 			pairMap.put(keypair1, pairMap.get(keypair1)+ 1);
			 		} else {
			 			pairMap.put(keypair1, 1);
			 		}
		    	}
		    }
		}
	    public void cleanup(Context context) throws IOException, InterruptedException{
	    	for (Entry<PairWritable, Integer> entry: pairMap.entrySet()){
	    		context.write(entry.getKey(), new IntWritable(entry.getValue()));
	    	}
	    }
		    
	} 
 	
	public static class Reduce extends Reducer<PairWritable, IntWritable, PairWritable, DoubleWritable> {

		private long total = 0;
		public void setup(Context context){
			total = 0;
		}
		public void reduce(PairWritable key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			long sum = 0; 
			for (IntWritable val : values) {
				sum += val.get();
			}
			if (key.getValue().equals("*")){
				total = sum;
			}else {
				context.write(key, new DoubleWritable((double)sum/total));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "inmappercombiningrfp" );
		job.setJarByClass(RelativeFreqPairInMapper.class);

		job.setMapOutputKeyClass(PairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(PairWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
 	
}
