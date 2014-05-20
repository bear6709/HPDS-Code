package org.hpds.multiedge.pagerank;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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

import org.hpds.multiedge.feature.feature_list;


public class feature {
	static int flag = 0;
	static Path[] localFiles;
	static int minsmallpacketsize = 0;
	static int maxsmallpacketsize = 0;
	public static String stime = "";
	public static String tinterval = ""; 
	
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
		//input: 140.116.164.71	19:13:05.053846 IP 140.116.164.71.11147 > 1.172.17.88.13484: UDP, length 1387 S
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			if( flag == 0){
				localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				BufferedReader dnslist = new BufferedReader( new FileReader(localFiles[0].toString()));
				while( dnslist.ready()){ 
					String tmpline = dnslist.readLine();
					String ip[] = tmpline.split(" ");
					if(ip[0].equals("minsmallpacketsize"))
						minsmallpacketsize = Integer.parseInt(ip[1]);
					if(ip[0].equals("maxsmallpacketsize"))
						maxsmallpacketsize = Integer.parseInt(ip[1]);
				}
				flag = 1;
			}
			
			if( value.getLength() > 0)
			{
				String line = value.toString();
				String ip[] = line.split("	");

				//long length = 0;
				if( line.indexOf("length") > 0){					
				
					String tmpline = line.substring( line.indexOf("length")+7, line.length()).trim();
					tmpline = tmpline.substring(0, tmpline.indexOf(" "));		//1387
					if( tmpline.matches("[0-9]+"))
					/*	length = Long.parseLong(tmpline)*/;
				
					//if( length > 0){
					String timestamp = ip[1].substring(0, ip[1].indexOf(" "));
					
					
				//*****************	not flow *****************************//	
				/*	String sp = "s" + ip[0];	//ratio of small packet
					if( minsmallpacketsize <= length && length <= maxsmallpacketsize){
						context.write( new Text(sp), new Text( timestamp + "_" + "small"));		//(sip , timestamp_small)						
					}else if( maxsmallpacketsize < length){
						context.write( new Text(sp), new Text( timestamp + "_" + "large"));		//(sip , timestamp_large)
					}*/		
					
					
					//if( minsmallpacketsize <= length && length <= maxsmallpacketsize){
						
				/*	String pps = "t" + ip[0];	// avg number of packet per sec
					context.write( new Text(pps), new Text( timestamp + "_" + timestamp));		//(tip , timestamp_timestamp)*/
						
				/*	String lps = "l" + ip[0];	// avg length of packet 
					context.write( new Text(lps), new Text( timestamp + "_" + length));		//(lip , timestamp_length)*/
						
				/*	String rps = "r" + ip[0];	// the ratio of incoming packets / outgoing packets
					if( line.charAt( line.length() - 1) == 'S'){
						context.write( new Text(rps), new Text( timestamp + "_" + "outgoing"));
					}else{
						context.write( new Text(rps), new Text( timestamp + "_" + "incoming"));
					}*/
				//*******************************************************//	
					
					
					String temp[] = ip[1].split(" ");	//average packet length per flow
					//ex: 19:13:05.053846 IP 140.116.164.71.11147 > 1.172.17.88.13484: UDP, length 1387 S
					String sflow = temp[2] + "_" + temp[4].substring( 0, temp[4].lastIndexOf(":"));		//140.116.164.71.11147_1.172.17.88.13484
					String dflow = temp[4].substring( 0, temp[4].lastIndexOf(":")) + "_" + temp[2];		
					
					if( line.charAt( line.length() - 1) == 'S'){
						//context.write( new Text( "f" + sflow), new Text( timestamp + "_" + length));
						//context.write( new Text( "f" + fps[2].substring( 0, fps[2].lastIndexOf(".")) + "_" + fps[4].substring( 0, fps[4].lastIndexOf("."))), new Text( timestamp + "_" + length));
						
						context.write( new Text( "t" + sflow),  new Text( timestamp + "_" + timestamp));		//( t140.116.164.71.11147_1.172.17.88.13484 , 19:13:05.053846_19:13:05.053846)
						
						context.write( new Text( "r" + sflow),  new Text( timestamp + "_" + "outgoing"));		//( r140.116.164.71.11147_1.172.17.88.13484 , 19:13:05.053846_outgoing)
						
						context.write( new Text( "i" + sflow), new Text( timestamp + "_" + timestamp));			//( i140.116.164.71.11147_1.172.17.88.13484 , 19:13:05.053846_19:13:05.053846)				
					}else{
						//context.write( new Text( "f" + dflow), new Text( timestamp + "_" + length));
						//context.write( new Text( "f" + fps[4].substring( 0, fps[4].lastIndexOf(".")) + "_" + fps[2].substring( 0, fps[2].lastIndexOf("."))), new Text( timestamp + "_" + length));
						
						context.write( new Text( "t" + dflow),  new Text( timestamp + "_" + timestamp));
						
						context.write( new Text( "r" + dflow),  new Text( timestamp + "_" + "incoming"));
						
						context.write( new Text( "i" + dflow), new Text( timestamp + "_" + timestamp));
					}
					
				/*	String nps = "n" + ip[0];	//the number of nulldata packets
					if( length == 0)
						context.write( new Text(nps), new Text( timestamp + "_" + timestamp));*/
					
				/*	String ups = "u" + ip[0];	//the number of nulldata packets
					if( length == 0)
						context.write( new Text(ups), new Text( timestamp + "_" + "nulldata"));
					else
						context.write( new Text(ups), new Text( timestamp + "_" + "data"));*/
					
				/*	String dps = "d" + ip[0];	//standard Deviation of packet length
					context.write( new Text(dps), new Text( timestamp + "_" + length));		//(lip , timestamp_length)*/
					
				/*	String ips = "i" + ip[0];	//number of dIP address interstitial time
					context.write( new Text(ips), new Text( timestamp + "_" + timestamp));*/
					
					//}		
					//}
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
			
			//ex: ( r140.116.164.71.11147_1.172.17.88.13484 , 19:13:05.053846_outgoing)
			Map<String,String> list = new HashMap<String,String>();	
			for( Text val : values) {
				String log[] = val.toString().split("_");
				list.put(log[0], log[1]);		//( 19:13:05.053846 , outgoing )
			}
			
			String[] rs = (String[])list.keySet().toArray(new String[0]);
			Arrays.sort(rs);		//sort by timestamp
			
			//for(String val : rs){
			//	context.write(key, new Text(val));
			//}
			//context.write(key, new Text(String.valueOf(stime)));		
			
			feature_list fl = new feature_list();
			
			switch( key.charAt(0)){		//"i" or "r" or "t" 
			
		/*	case 's':				
				fl.smallpacket( key, list, rs, context);
			break;*/
			
			case 't': 
				fl.packetpersec( key, list, rs, context);
			break;
			
		/*	case 'l':
				fl.avglength( key, list, rs, context);
			break;	*/
			
			case 'r':
				fl.ioratio( key, list, rs, context);
			break;
			
		/*	case 'f':
				fl.avglengthperflow( key, list, rs, context);
			break;*/
			
		/*	case 'n':
				fl.nullpacketnum( key, list, rs, context);
			break;*/
			
		/*	case 'd':
				fl.standarddeviation( key, list, rs, context);
			break;*/
			
			case 'i':
				fl.avginterstitial( key, list, rs, context);
			break;
			
			}	
			/*
			for( String val : rs) {
				context.write(key, new Text(val));
			}*/
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
