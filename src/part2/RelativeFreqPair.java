package part2;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import part1.Pair;



 	
public class RelativeFreqPair {
 	
	public static class PairWritable extends Pair<String, String>  implements WritableComparable<PairWritable>{

			public PairWritable(){
				super("", "");
			}
			public PairWritable(String key, String value){
				super(key, value);
			}

			public void set (String key, String value){
				setKey(key);
				setValue(value);
			}
			@Override
			public void readFields(DataInput in) throws IOException {
				// TODO Auto-generated method stub
				 setKey(in.readUTF());
			     setValue(in.readUTF());
				
			}
			@Override
			public void write(DataOutput out) throws IOException {
				// TODO Auto-generated method stub
				out.writeUTF(getKey());
		        out.writeUTF(getValue());
			}

			@Override
			public int compareTo(PairWritable other) {
				// TODO Auto-generated method stub
				int cmp = this.getKey().compareTo(other.getKey());
				if (cmp != 0) return cmp;
				return this.getValue().compareTo(other.getValue());
			}
			@Override
			public int hashCode(){
				return this.getKey().hashCode()*163 + this.getValue().hashCode();
			}
			@Override
			public boolean equals(Object ob){
				if ( ob instanceof PairWritable){
					PairWritable other = (PairWritable)ob;
					return this.getKey().equals(other.getKey()) &&
							this.getValue().equals(other.getValue());
				}
				return false;
			}
			@Override
			public String toString(){
				return this.getKey() + "," + this.getValue();
			}
	}
	
	public static class Map extends Mapper<LongWritable, Text, PairWritable, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
		    String []tokens = line.split(" ");
		    for (int i = 0; i< tokens.length-1; i++){
		    	for(int j=i+1; j< tokens.length; j++){
		    		String item1 = tokens[i];
		    		String item2 = tokens[j];
		    		if (item1.equalsIgnoreCase(item2)) break;
		    		PairWritable pair = new PairWritable(item1, item2);
		    		context.write(pair, one);
		    	}
		    }
		}
		    
	} 
 	
	public static class Reduce extends Reducer<PairWritable, IntWritable, PairWritable, LongWritable> {

		public void reduce(PairWritable key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			long sum = 0; 
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "relativefreqpair" );
		job.setJarByClass(RelativeFreqPair.class);

		job.setMapOutputKeyClass(PairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(PairWritable.class);
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
