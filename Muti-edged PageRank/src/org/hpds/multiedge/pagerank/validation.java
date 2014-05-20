package org.hpds.multiedge.pagerank;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class validation {
	
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
		static String bothost = "140.116.2.";
	
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		
			String line[] = value.toString().split("	");
			if( line[1].indexOf(bothost) >= 0){
				if( line[0].indexOf("bot") == 0){
					context.write(new Text("bot"), new Text("TP"));
				}else
					context.write(new Text("bot"), new Text("FN"));
			}else{
				if( line[0].indexOf("bot") == 0){
					context.write(new Text("normal"), new Text("TN"));
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int TP = 0, FN = 0, TN = 0;
			
			if( key.toString().equals("bot")){
				for(Text val : values) {
					if( val.toString().equals("TP"))
						TP++;
					else
						FN++;
				}
				BigDecimal sum = new BigDecimal(TP).add(new BigDecimal(FN));
				String result = String.format("TP: %-6d  FN: %-6d  TP-rate: %-6f", TP, FN, new BigDecimal(TP).divide(sum, 6, BigDecimal.ROUND_HALF_UP).doubleValue());
				context.write(key, new Text(result));
			}else{
				for(Text val : values) {
					if( val.toString().equals("TN"))
						TN++;
				}
				context.write(key, new Text("TN: " + TN));
			}
			
		}
	}

	public static void main(String[] args) throws Exception {
		 
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		//DistributedCache.addCacheFile(new URI("/user/hpds/config"),conf);

		Job job = new Job(conf, "result");
		job.setJarByClass(result.class);

		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);	
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));			
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
