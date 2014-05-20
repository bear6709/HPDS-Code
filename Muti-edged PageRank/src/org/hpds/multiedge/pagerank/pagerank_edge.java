package org.hpds.multiedge.pagerank;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.StringTokenizer;

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

public class pagerank_edge {
	
	static int flag = 0;
	static Path[] localFiles;

	
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {

		
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			//0_140.116.164.71        :[1,1,1]:0_140.116.164.91[0,14,0]|0_140.116.164.97[0,9,0]
			String line[] = value.toString().split(":");
			
			
			if( line.length >= 3){			
				
				String initline[] = value.toString().split("	");
				context.write( new Text(initline[0]), new Text( "z_" + line[2]));		//pagerank loop data
				
				StringTokenizer vElement = new StringTokenizer( line[2], "|");		//split edge
				

				String one = "1";
				while (vElement.hasMoreTokens()) {
					
					String ip[] = vElement.nextToken().split("\\[");		//0,14,0]
				

						context.write( new Text(ip[0]), new Text("i_" + one));
						context.write( new Text(ip[0]), new Text("r_" + one));
						context.write( new Text(ip[0]), new Text("t_" + one));

				}			
				
				
			}			
			
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			
			BigDecimal suml = new BigDecimal(0);
			BigDecimal sums = new BigDecimal(0);
			BigDecimal sumt = new BigDecimal(0);
			String initline = "";
			
			for (Text val : values) {
				
				String line[] = val.toString().split("_");
				BigDecimal tmp = new BigDecimal( line[1].toString());
				
				if( line[0].equals("i"))
					suml = suml.add(tmp);
				else if( line[0].equals("r"))
					sums = sums.add(tmp);
				else if( line[0].equals("t"))
					sumt = sumt.add(tmp);
				else if( line[0].equals("z"))
					initline = val.toString().substring(2, val.getLength());
				
			}
			String result = ":[" + suml + "," + sums + "," + sumt + "]:" + initline;
			context.write( key, new Text(result));
			
		
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		
		//DistributedCache.addCacheFile(new URI("/user/hpds/pagerank"),conf);
		
		FileSystem fs = FileSystem.get( conf);
		if( fs.exists(new Path("/user/hpds/featurelist")))
			fs.deleteOnExit( new Path("/user/hpds/featurelist"));		//remove featurelist temp file
		
		Job job = new Job(conf, "pagerank");

		job.setJarByClass(pagerank_edge.class);
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
