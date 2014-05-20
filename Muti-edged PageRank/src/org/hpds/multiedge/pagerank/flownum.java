package org.hpds.multiedge.pagerank;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
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

import org.hpds.multiedge.feature.flownum_list;

public class flownum {
	static int flag = 0;
	static Path[] localFiles;
	static Set<String> ipset;
	public static String stime = "";
	public static String tinterval = ""; 
	
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
		//input: 140.116.164.71	19:13:05.053846 IP 140.116.164.71.11147 > 1.172.17.88.13484: UDP, length 1387 S
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			if( flag == 0){
				localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				BufferedReader dnslist = new BufferedReader( new FileReader(localFiles[0].toString()));
				ipset = new HashSet<String>();
				while( dnslist.ready()){ 
					String tmpline = dnslist.readLine();
					String ip[] = tmpline.split(" ");
					if(ip[0].equals("host"))
						ipset.add( ip[1]);
				}
				flag = 1;
			}
			
			if( value.getLength() > 0)
			{
				String line = value.toString();
				String ip[] = line.split(" ");
				String sip = null, dip = null;
				boolean tmp = true;
				
				if( ip.length > 5 && ip[1].equals("IP")){				
					
					if( ip[2].split("\\.").length == 5)
						sip = ip[2].substring( 0, ip[2].lastIndexOf("."));
					
					if( ip[4].split("\\.").length == 5)
						dip = ip[4].substring(0, ip[4].lastIndexOf("."));	
					
					if( sip != null && dip != null ){
						
					//	if( Integer.parseInt(sip.substring(0, sip.indexOf("."))) < 224 && Integer.parseInt(dip.substring(0, dip.indexOf("."))) < 224){ //ip > 224 no C&D class
					//		Text tmps = new Text("S");
					//		Text tmpd = new Text("R");
							
							Iterator<String> host = ipset.iterator();
							while( host.hasNext()){
								String hostip = host.next();
								if( sip.indexOf(hostip) == 0){  //ipset=140.116.164 sip:140.116.164.98=0->true  sip:130.140.116.164=5->false
									if( dip.indexOf(hostip) < 0){
										tmp = true;
									//	context.write( new Text(sip), new Text(line));
									}
									break;									
								}else if( dip.indexOf(hostip) == 0){
									tmp = false;
								//	context.write( new Text(dip), new Text(line));
									break;
								}
							}
								//context.write( new Text("first"), new Text(ip[0]));
				//		}						
					}
				//}				

				//long length = 0;
				//if( line.indexOf("length") > 0){				
					
					String ip1 = line;
					
					String timestamp = ip1.substring(0, ip1.indexOf(" "));			
					String temp[] = ip1.split(" ");	//average packet length per flow
					//ex: 19:13:05.053846 IP 140.116.164.71.11147 > 1.172.17.88.13484: UDP, length 1387 S
					String sflow = temp[2] + "_" + temp[4].substring( 0, temp[4].lastIndexOf(":"));		//140.116.164.71.11147_1.172.17.88.13484
					String dflow = temp[4].substring( 0, temp[4].lastIndexOf(":")) + "_" + temp[2];		
					
					if( tmp == true){
						
						context.write( new Text(sflow),  new Text( timestamp + "_" + timestamp));		//( t140.116.164.71.11147_1.172.17.88.13484 , 19:13:05.053846_19:13:05.053846)
										
					}else{
						
						context.write( new Text(dflow),  new Text( timestamp + "_" + timestamp));
						
					}
					
				
				}
			}
			
			
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			
			if( flag == 0){
				localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				BufferedReader dnslist = new BufferedReader( new FileReader(localFiles[1].toString()));
				while( dnslist.ready()){ 
					String tmpline = dnslist.readLine();
					String ip[] = tmpline.split(" ");
					if( ip[0].equals("stime"))
						stime = ip[1];			
				}
				dnslist = new BufferedReader( new FileReader(localFiles[0].toString()));
				while( dnslist.ready()){ 
					String tmpline = dnslist.readLine();
					String ip[] = tmpline.split(" ");
					if( ip[0].equals("interval"))
						tinterval = ip[1];			
				}
				flag = 1;
			}
			
			Map<String,String> list = new HashMap<String,String>();	
			for( Text val : values) {
				String log[] = val.toString().split("_");
				list.put(log[0], log[1]);		//( 19:13:05.053846 , outgoing )
			}
			
			String[] rs = (String[])list.keySet().toArray(new String[0]);
			Arrays.sort(rs);
			
			flownum_list f2 = new flownum_list();
			
			f2.packetpersec( key, list, rs, context);
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
		DistributedCache.addCacheFile(new URI("/user/hpds/stime"),conf);
		
		Job job = new Job(conf, "feature");

		job.setJarByClass(feature.class);
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
