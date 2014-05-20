package org.hpds.multiedge.pagerank;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class pager {

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			
			String line = value.toString();			
			String sip[] = line.split(":");
			BigDecimal current_score = new BigDecimal(sip[1]);
			if( sip.length >= 3) {
				StringTokenizer vElement = new StringTokenizer(sip[2], ",");
				BigDecimal len_adjlist = new BigDecimal(vElement.countTokens());
				BigDecimal subrank = current_score.divide(len_adjlist, 8, BigDecimal.ROUND_HALF_UP);
				//subrank = subrank.setScale(2, BigDecimal.ROUND_HALF_UP);
				while (vElement.hasMoreTokens()) {
					if(line.charAt(0)=='h')
						context.write(new Text("h" + vElement.nextToken()), new Text(subrank.toString()));	
					else
						context.write(new Text("a" + vElement.nextToken()), new Text(subrank.toString()));
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			BigDecimal sum = new BigDecimal("0");

			for (Text val : values) {
				BigDecimal tmp = new BigDecimal(val.toString());
				sum = sum.add(tmp);
			}
			
			//sum = sum.setScale(3, BigDecimal.ROUND_HALF_UP);
			//BigDecimal subrank = new BigDecimal((double) (sum));
			//subrank = subrank.setScale(2, BigDecimal.ROUND_HALF_UP);
			
			result.set(sum.doubleValue());
			if(key.toString().indexOf("h140.116") ==0 || key.toString().indexOf("a140.116") ==0)
				context.write(key, result);
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: pagerank <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "page rank");
		job.setJarByClass(pager.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		/*
		for(int i=1;i<5000;i++){
		BigDecimal subrank = new BigDecimal((double)(1.0/i));
		subrank = subrank.setScale(2, BigDecimal.ROUND_HALF_UP);
		
		System.out.println(subrank.doubleValue());
		//System.out.println((double)(1.0/5));
		}*/

	}
}
