package org.hpds.multiedge.pagerank;

import java.io.BufferedReader;
import java.io.FileReader;
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

public class ipresult {
	
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
	
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		
			if( value.getLength() > 0){
				String line[] = value.toString().split("_");
				String ip = line[2].substring(0,line[2].lastIndexOf("."));
				context.write(new Text(ip), new Text(line[0]));
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		static int flag = 0;
		static Path[] localFiles;
		static float resultratio = 0;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if( flag == 0){
				localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				BufferedReader list = new BufferedReader( new FileReader(localFiles[0].toString()));
				while( list.ready()){ 
					String tmpline = list.readLine();
					String ip[] = tmpline.split(" ");
					if(ip[0].equals("resultratio"))
						resultratio = new Integer(ip[1]);	
				}
			}		
			
			int bot = 0, normal = 0;
			for(Text val : values) {
				if( val.toString().equals("normal"))
					normal++;
				else
					bot++;
			}
			BigDecimal ipresult = new BigDecimal(bot).divide(new BigDecimal(bot + normal), 5, BigDecimal.ROUND_HALF_UP);
			String output = "bot: "+ bot + " normal: " + normal + " ratio: "+ ipresult;
			context.write(key, new Text(output));
			/*if( ipresult >= resultratio)
				context.write(key, new Text("bot"));
			else
				context.write(key, new Text("normal"));*/
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

		Job job = new Job(conf, "ipresult");
		job.setJarByClass(ipresult.class);

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
