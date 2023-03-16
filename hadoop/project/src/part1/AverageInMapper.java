package part1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class AverageInMapper {

	public static class PairWritable extends Pair<Integer,Integer> implements Writable{
		PairWritable() {super(0, 0);}
		PairWritable(Integer _key, Integer _value) {super(_key, _value);}
	
		public PairWritable add(PairWritable _pair){
			this.setKey(this.getKey() + _pair.getKey());
			this.setValue(this.getValue() + _pair.getValue());
			return this;
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			 setKey(in.readInt());
		     setValue(in.readInt());
			
		}
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(getKey());
	        out.writeInt(getValue());
		}
	}
	
	 public static class Map extends Mapper<LongWritable, Text, Text, PairWritable> {
			
	    private HashMap<String, PairWritable> map;
	    
	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	      super.setup(context);
	      map = new HashMap<>();
	    }
	 	
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	  
	    	String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
		    // Get the first and last item from the line
		    String firstItem = tokenizer.nextToken();
		    String lastItem = "";
		    
			while (tokenizer.hasMoreTokens()) {
					lastItem = tokenizer.nextToken();
			}
			if (!Average.isValidIP(firstItem) || !Average.isNumeric(lastItem)) return;
			
			PairWritable pair = new PairWritable(Integer.parseInt(lastItem),1);
			
			if(map.containsKey(firstItem)) pair.add(map.get(firstItem));
				
			map.put(firstItem, pair);	
	    }
	    
	    @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	      super.cleanup(context);
	      // Emit intermediate key-value pairs
	      for (Entry<String, PairWritable> entry : map.entrySet()) {
	        context.write(new Text(entry.getKey()), entry.getValue());
	        //System.out.println(entry.getKey() + "--------" + entry.getValue().getKey());
	      }
	    }
	 } 
	 
	
	 public static class Reduce extends Reducer<Text, PairWritable, Text, DoubleWritable> {
	
	    public void reduce(Text key, Iterable<PairWritable> values, Context context) 
	      throws IOException, InterruptedException {
	        Double sum = 0.0;
	        Integer count = 0;
		 	for (PairWritable val : values) {
		 	    sum += val.getKey();
		 	    count += val.getValue();
		 	}
		 	context.write(key, new DoubleWritable(sum/count));
	    }
	 }
	 	
	 
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Average Computation in-mapper combining");
		job.setJarByClass(AverageInMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PairWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
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
		 String filePath = "input/average/access_log";
		 String[] input_splits = new String[1];
			try {
				byte[] bytes = Files.readAllBytes(Paths.get(filePath));
				input_splits[0] = new String(bytes);
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}	
			
			HashMap<String, PairWritable> map = new HashMap<>();
	 
		 try (Scanner scanner = new Scanner(input_splits[0])) {
				while (scanner.hasNextLine()) {
					String  line = scanner.nextLine();
					String[] tokens = line.split(" - - ");
					if(tokens.length == 0)	continue;
					if(!tokens[0].matches("\\d+.\\d+.\\d+.\\d+")) continue;
					
					String ip = tokens[0];
					PairWritable pair = new PairWritable(Integer.parseInt(tokens[tokens.length-1]),1);
					if(map.containsKey(ip)) pair.add(map.get(ip));
					
					map.put(ip, pair);	
					
				}
				
			}catch(Exception e) {
				System.out.println(e.toString());
			}
		 
		 return;
	 }*/
}
