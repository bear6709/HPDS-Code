package org.hpds.multiedge.pagerank;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

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

public class combine {
	
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			//s0_140.116.164.91       s0_140.116.164.91:s0_140.116.164.97[9],s0_140.116.164.71[16]
			if( value.getLength() > 0){		
				String line[] = value.toString().split("	");
				String val[] = line[1].split(":");
				String keyout = val[0].substring( 1, val[0].length());
				if( val.length >= 2){
					StringTokenizer vElement = new StringTokenizer(val[1], ",");
					while ( vElement.hasMoreTokens()) {						
						context.write( new Text(keyout), new Text(vElement.nextToken()));		//(1_140.116.164.97 , l1_140.116.164.91[394]) , (...) , ...				
					}
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			Map<String,String> list = new HashMap<String,String>();	
			for( Text val : values){
				String line[] = val.toString().split("\\[");
				//String ip = line[0].substring( line[0].indexOf("_") + 1, line[0].length());
				String ip = line[0].substring( 1, line[0].length());
				String value = " ";
				if( val.charAt(0) == 's')
					value = "s" + line[1].substring(0, line[1].indexOf("]"));
				else if( val.charAt(0) == 't')
					value = "t" + line[1].substring(0, line[1].indexOf("]"));
				else
					value = "l" + line[1].substring(0, line[1].indexOf("]"));
				if( list.containsKey(ip)){		//if ip already in the list -> append value
					String tmp = list.get(ip);
					tmp = tmp + value + ",";
					list.put( ip, tmp);
				}else
					list.put(  ip, value + ",");
			}
			String[] rs = (String[])list.keySet().toArray(new String[0]);
			String output = ":[1,1,1]:";
			for( String val : rs){
				//output += val + "[" + list.get(val) + "],";		//ip1[f1,f2,f3] , ip2[f1,f2,f3] , ...
				output += val + "[";
				
				int ind = list.get(val).indexOf("l");
				if( ind >= 0){
					String temp = list.get(val).substring( ind +1 , list.get(val).length());
					output += temp.substring(0 , temp.indexOf(",")) + ",";
				}else
					output += "0,";
				
				ind = list.get(val).indexOf("s");
				if( ind >= 0){
					String temp = list.get(val).substring( ind +1 , list.get(val).length());
					output += temp.substring(0 , temp.indexOf(",")) + ",";
				}else
					output += "0,";
				
				ind = list.get(val).indexOf("t");
				if( ind >= 0){
					String temp = list.get(val).substring( ind +1 , list.get(val).length());
					output += temp.substring(0 , temp.indexOf(",")) + "]|";
				}else
					output += "0]|";
			}
			if( output.charAt(output.length() - 1) == '|')		//remove the last , 
				output = output.substring( 0, output.length() - 1);
			context.write( key, new Text(output));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "combine");

		job.setJarByClass(pagerank.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
