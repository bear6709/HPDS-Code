package org.hpds.multiedge.pagerank;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class cofeature {
	
	static BigDecimal avginterst = new BigDecimal(0.5);
	static BigDecimal ior = new BigDecimal(55);
	static BigDecimal packetsec = new BigDecimal(3);
	
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			if( value.getLength() > 0){
				String line[] = value.toString().split("	");
				String keyout = line[0].substring( 1,line[0].length());		//0_140.116.164.83.5173_111.255.1.215.56931
				if( value.charAt(0) == 'i'){
					String valout = "i" + line[1].substring( line[1].indexOf(":") + 1, line[1].length()).trim();
					context.write( new Text(keyout), new Text(valout));		//l0_140.116.164.83.5173_111.255.1.215.56931	length/packet: 41 -> ( 0_140.116.164.83.5173_111.255.1.215.56931 , l41 )
				}else if( value.charAt(0) == 'r'){
					String valout = "r" + line[1].substring( line[1].indexOf("ratio:") + 6, line[1].length()).trim();
					context.write( new Text(keyout), new Text( valout.substring(0, valout.length() - 1)));		//-%
				}else if( value.charAt(0) == 't'){
					String valout = "t" + line[1].substring( line[1].indexOf("packets/sec:") + 12, line[1].length()).trim();		//t0_140.116.164.83.5173_111.255.1.215.56931	packets/sec: 0.03 -> ( 0_140.116.164.83.5173_111.255.1.215.56931 , t0.03)
					context.write( new Text(keyout), new Text(valout));
				}
			}						
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
		
			String temp = "";
			for( Text val : values) {
				temp += val + ",";		//ex: i0.8,r60,t5
			}
			//context.write(key, new Text(temp));
			String result = "";
			
			//int ind = temp.indexOf("l");
			
			boolean isbot = true;
			
			if( temp.indexOf("i") >= 0){
				String ind = temp.substring( temp.indexOf("i") + 1, temp.length());
				String i = ind.substring( 0 , ind.indexOf(","));
				result += i + ",";
				if( new BigDecimal(i).compareTo( avginterst) == -1)	//avginterstitial < 0.5 is not bot
					isbot = false;
			}else
				result += "-1,";
			
			if( temp.indexOf("r") >= 0){
				String ind = temp.substring( temp.indexOf("r") + 1, temp.length());
				String r = ind.substring( 0 , ind.indexOf(","));
				result += r + ",";
				if( new BigDecimal(r).compareTo( ior) == 1)		//ioratio > 55% is not bot
					isbot = false;
			}else
				result += "-1,";
			
			if( temp.indexOf("t") >= 0){
				String ind = temp.substring( temp.indexOf("t") + 1, temp.length());
				String t = ind.substring( 0 , ind.indexOf(","));
				result += t;
				if( new BigDecimal(t).compareTo( packetsec) == 1)		//packetpersec > 3 is not bot
					isbot = false;
			}else
				result += "-1";
			
			if( result.charAt(result.length() - 1) == ',')
				result = result.substring( 0, result.length() - 1);
			
			if( isbot == true)
				context.write(key, new Text(result));		//ex: 0_140.116.164.83.5173_111.255.1.215.56931	4.62,49,0.2122
				
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		
		FileSystem fs = FileSystem.get( conf);
		if( fs.exists(new Path("/user/hpds/stime")))
			fs.deleteOnExit( new Path("/user/hpds/stime"));		//remove timestamp temp file
		
		Job job = new Job(conf, "cofeature");

		job.setJarByClass(cofeature.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setCombinerClass(Combiner.class);
		job.setReducerClass(IntSumReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
