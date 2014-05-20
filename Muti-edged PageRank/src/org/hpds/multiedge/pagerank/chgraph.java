package org.hpds.multiedge.pagerank;

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

public class chgraph {

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {

	//	static int flag = 0;
	//	static Path[] localFiles;
	//	static Set<String> ipset;
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			
		/*	if( flag == 0){
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
			}*/
			
			if( value.getLength() > 0)
			{
				String line = value.toString();
				String ip[] = line.split(" ");
				
			
				if( ip.length > 5 && ip[1].equals("IP")){				
				
					/**********************************IP**********************************/
					String sip = null, dip = null;
					if( ip[2].split("\\.").length == 5)
						sip = ip[2].substring( 0, ip[2].lastIndexOf("."));
					
					if( ip[4].split("\\.").length == 5)
						dip = ip[4].substring(0, ip[4].lastIndexOf("."));
					
					context.write(new Text("h"+sip), new Text(dip));
					context.write(new Text("a"+dip), new Text(sip));
					/**********************************************************************/
					
					/**********************************flow********************************/
			/*		String sflow = null, dflow = null;
					if( ip[2].split("\\.").length == 5)
						sflow = ip[2];
					
					if( ip[4].split("\\.").length == 5)
						dflow = ip[4].substring(0, ip[4].length()-1);
					
					context.write(new Text("h"+sflow), new Text(dflow));
					context.write(new Text("a"+dflow), new Text(sflow));	*/				
					/*********************************************************************/
							
				/*	Iterator<String> host = ipset.iterator();
					while( host.hasNext()){
						String hostip = host.next();
						if( sip.indexOf(hostip) == 0){  //ipset=140.116.164 sip:140.116.164.98=0->true  sip:130.140.116.164=5->false
							if( dip.indexOf(hostip) < 0){
								context.write(new Text(dip), new Text(sip));
							}
							break;
						}else if( dip.indexOf(hostip) == 0){
							context.write(new Text(sip), new Text(dip));
							break;
						}
					}*/
				}	
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			String tmp = new String(":1:");
			Set<String> set = new HashSet<String>();

			
			if (values.iterator().hasNext()) {
				String str = values.iterator().next().toString();
				tmp += str;
				set.add(str);
				while (values.iterator().hasNext()) {
					str = values.iterator().next().toString();
					if(!set.contains(str)){
						set.add(str);
						tmp += ",";
						tmp += str;
					}else{
						continue;
					}				
				}
			}
			result.set(tmp);
			context.write(key, result);
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
		Job job = new Job(conf, "chgraph");

		job.setJarByClass(chgraph.class);
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
