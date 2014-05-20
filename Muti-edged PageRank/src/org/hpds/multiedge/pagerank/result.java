package org.hpds.multiedge.pagerank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class result {
	
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		static int flag = 0;
		static Path[] localFiles;
		static BigDecimal split = new BigDecimal(0);
		
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		
			if( flag == 0){
				localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				BufferedReader list = new BufferedReader( new FileReader(localFiles[0].toString()));
				while( list.ready()){ 
					String tmpline = list.readLine();
					String ip[] = tmpline.split(" ");
					if(ip[0].equals("split"))
						split = new BigDecimal(ip[1]);
					
				}
			}
			
			StringBuffer temp = new StringBuffer("");
			if( value.getLength() > 0){
				String line[] = value.toString().split(",");
				BigDecimal entropy = new BigDecimal(line[3]);
				
				temp.append(Math.floor(entropy.divide(split, 3, BigDecimal.ROUND_HALF_UP).doubleValue()));
				//temp.append(entropy.divide(split, 3, BigDecimal.ROUND_HALF_UP));
				context.write( new Text(temp.toString()), value);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		static int flag = 0;
		static Path[] localFiles;
		static int bot_threshold = 0;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if( flag == 0){
				localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				BufferedReader list = new BufferedReader( new FileReader(localFiles[0].toString()));
				while( list.ready()){ 
					String tmpline = list.readLine();
					String ip[] = tmpline.split(" ");
					if(ip[0].equals("bot_threshold"))
						bot_threshold = new Integer(ip[1]);
					
				}
			}
			
			int num = 0;;
			BufferedWriter bufWriter = new BufferedWriter(new FileWriter("/opt/hadoop/hpdstmp2.txt"));
			for(Text val : values) {
				String line = val.toString();
				bufWriter.write(line);
				bufWriter.newLine();
				
				num++;	
			}
			bufWriter.close();

			BufferedReader br = new BufferedReader( new FileReader("/opt/hadoop/hpdstmp2.txt"));
			String line;
			if( num >= bot_threshold){
				while((line = br.readLine()) != null){
					context.write(new Text("bot_" + key), new Text(line));
				}
			}else{
				while((line = br.readLine()) != null){
					context.write(new Text("normal_" + key), new Text(line));
				}
			}
			br.close();
			
			File f1 = new File("/opt/hadoop/hpdstmp2.txt");
			f1.delete();
		}
	}

	public static void main(String[] args) throws Exception {
		 
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		DistributedCache.addCacheFile(new URI("/user/hpds/config"),conf);

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
