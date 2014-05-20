package org.hpds.multiedge.pagerank;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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

public class failedcon_time {
	
	static int flag = 0;
	static Path[] localFiles;
	static Set<String> dns;
	static String ipset;
	static long interval = 99999;
	
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
					if(ip[0].equals("interval"))
						interval = new Integer(ip[1]); 
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
					
					if( sip != null && dip != null){
						if(!dns.contains(sip) && !dns.contains(dip)){
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

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			Map<String,String> list = new HashMap<String,String>();	
			for (Text val : values) {
				String line = val.toString();
				String ip[] = line.split(" ");
				list.put(ip[0], line);
			}
			
			String[] rs = (String[])list.keySet().toArray(new String[0]);
			Arrays.sort(rs);  //sort ip+timestamp
			
			Map<String,String> sc = new HashMap<String,String>();	
			Map<String,String> fc = new HashMap<String,String>();
			
			for (String val : rs) {
				String line = list.get(val);
				String ip[] = line.split(" ");
				String sip = ip[2];
				String dip = ip[4].substring(0, ip[4].length()-1);
				
				SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");		
				long quot = 0;
				
				if( line.charAt(line.length()-1) == 'S'){
					if( !sc.containsKey(dip)){
						if( !fc.containsKey(dip))
							fc.put(dip, ip[0]);
					}else{						
						sc.remove(dip);
						fc.put(dip, ip[0]);
					}
				}else{
					if( fc.containsKey(sip)){
						try{
							Date date1 = df.parse(fc.get(sip));
						    Date date2 = df.parse(ip[0]);
						    quot = ( date2.getTime() - date1.getTime()) / 1000;					    
						}catch (ParseException e){
						    e.printStackTrace();
						}
						if( quot < interval){
							fc.remove(sip);
							sc.put(sip, ip[0]);
						}
					}else 
						sc.put(sip, ip[0]);
				}												
				//context.write(key, new Text(val));
			}
			BigDecimal f = new BigDecimal(fc.size());
			BigDecimal s = new BigDecimal(sc.size());
			BigDecimal ratio = f.divide(f.add(s), 5, BigDecimal.ROUND_HALF_UP);
			String str = String.format("snum: %-5d  fnum: %-5d  ratio: %-3f", sc.size(), fc.size(), ratio);
			context.write(key, new Text(str));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: pagerank <in> <out>");
			System.exit(2);
		}
		
		DistributedCache.addCacheFile(new URI("/user/hpds/dns"),conf);
		DistributedCache.addCacheFile(new URI("/user/hpds/config"),conf);

		Job job = new Job(conf, "failed connections");
		job.setJarByClass(failedcon.class);

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
