package org.hpds.multiedge.pagerank;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

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

public class P2Plist {
	
	static int flag = 0;
	static Path[] localFiles;
	static Set<String> dns;
	static Set<String> p2plist;
	static String ipset;
	static long threshold;
	
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		
			if( flag == 0){
				localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				
				BufferedReader dnslist = new BufferedReader( new FileReader(localFiles[0].toString()));
				dns = new HashSet<String>();
				while( dnslist.ready()){ 
					String dnsline = dnslist.readLine();
					dns.add(dnsline);
				}
				dnslist = new BufferedReader( new FileReader(localFiles[1].toString()));
				while( dnslist.ready()){ 
					String tmpline = dnslist.readLine();
					String ip[] = tmpline.split(" ");
					if(ip[0].equals("host"))
						ipset = ip[1];
				}
				dnslist = new BufferedReader( new FileReader(localFiles[2].toString()));
				p2plist = new HashSet<String>();
				while( dnslist.ready()){ 
					String p2pline = dnslist.readLine();
					String ip[] = p2pline.split("	");
					p2plist.add(ip[0]);
				}
				flag = 1;
			}
			
			if( value.getLength() > 0)
			{
				String line = value.toString();
				String ip[] = line.split(" ");
				String sip = null, dip = null;
			
				if( ip.length > 5 && ip[1].equals("IP")){				
							
					if( ip[2].split("\\.").length == 5)
						sip = ip[2].substring( 0, ip[2].lastIndexOf("."));
					
					if( ip[4].split("\\.").length == 5)
						dip = ip[4].substring(0, ip[4].lastIndexOf("."));	
					
					if( sip != null && dip != null && (p2plist.contains(sip) || p2plist.contains(dip))){
						if(!dns.contains(sip) && !dns.contains(dip)){
							if( Integer.parseInt(sip.substring(0, sip.indexOf("."))) < 224 && Integer.parseInt(dip.substring(0, dip.indexOf("."))) < 224){ //ip > 224 no C&D class
								Text tmps = new Text(" S");
								Text tmpd = new Text(" R");
							
								if( sip.indexOf(ipset) == 0){  //ipset=140.116.164 sip:140.116.164.98=0->true  sip:130.140.116.164=5->false
									if( dip.indexOf(ipset) < 0){
										line += tmps;
										context.write( new Text(sip), new Text(line));
									}
								}else if( dip.indexOf(ipset) == 0){
									line += tmpd;
									context.write( new Text(dip), new Text(line));
								}
							}
						}
					}
				}	
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			for( Text val : values) {
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
		DistributedCache.addCacheFile(new URI("/user/hpds/dns"),conf);
		DistributedCache.addCacheFile(new URI("/user/hpds/config"),conf);
		DistributedCache.addCacheFile(new URI("/user/hpds/p2plist"),conf);

		Job job = new Job(conf, "failed connection");
		job.setJarByClass(failedcon.class);

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
