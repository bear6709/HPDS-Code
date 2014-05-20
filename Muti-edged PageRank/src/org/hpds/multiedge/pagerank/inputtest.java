//pagerank initial score: rescale
//StringBuffer
package org.hpds.multiedge.pagerank;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
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

public class inputtest {
	
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
		static int flag = 0;
		static Path[] localFiles;
		static BigDecimal avginterstitial = new BigDecimal(0);
		static BigDecimal ioratio = new BigDecimal(0);
		static BigDecimal packetpersec = new BigDecimal(0);
		static Map<String,List<String>> feature;
		static BigDecimal []maxfeature = {new BigDecimal(0),new BigDecimal(0),new BigDecimal(0)};
		static BigDecimal newscale = new BigDecimal(100);

		@SuppressWarnings("deprecation")
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			
			if( flag == 0){
			//	localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			//	BufferedReader list = new BufferedReader( new FileReader(localFiles[0].toString()));
				
				
				FileSystem dfs = FileSystem.get(context.getConfiguration());
				Path path = new Path(context.getConfiguration().get("inputstring"));
				FileStatus[] files = dfs.listStatus(path);
				
				feature = new HashMap<String,List<String>>();		//time interval 2-array				
						
				//while( list.ready()){
				//System.out.println("abccd"+localFiles[0].toString());
				//System.out.println("localFiles[1].toString()"+path);
				for (int i = 0; i < files.length; i++) {
					if (!files[i].isDir()) {
						FSDataInputStream is = dfs.open(files[i].getPath());
                        String line = "";
                        while ((line = is.readLine()) != null) {
						
							//String line = list.readLine();
							int index = line.indexOf("_");
							String keys = line.substring( 0, index);
							//String Value = line.substring(index);
							List<String> tmp;
							
							if(feature.containsKey(keys)){		//if this time interval exist
								tmp = feature.get(keys);
								tmp.add(line);
								feature.put( keys, tmp);
							}else{
								tmp = new ArrayList<String>();
								tmp.add(line);
								feature.put( keys, tmp);
							}				
							
                        }
					}
				}
				
				flag = 1;
			}			
			
			
				String line[] = value.toString().split("	");
				String sline = value.toString();
				
				
				String timeslot = line[0].substring( 0, line[0].indexOf("_"));
				
				String[] rs = (String[])feature.get(timeslot).toArray(new String[0]);
				for( String val : rs){
					
					//if( val.substring( 0, val.indexOf("_")).equals( line[0].substring( 0, line[0].indexOf("_")))){		//if in the same time interval
					String fline = val.toString();
					if(sline.equals(fline)){
						String tmp[] = val.toString().split("	");
						context.write( new Text(line[0]), new Text(tmp[1]));
					}
					
				}
				
	
				
			
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			for( Text val : values){
				context.write(key, val);
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
		
		DistributedCache.addCacheFile(new URI("/user/hpds/config"),conf);
		//DistributedCache.addCacheFile(new URI("/user/hpds/featurelist"),conf);
		//DistributedCache.addCacheFile(new URI(args[0]),conf);
		conf.set("inputstring", args[0]); 		

		Job job = new Job(conf, "graph3_rtest");
		job.setJarByClass(graph2.class);
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
