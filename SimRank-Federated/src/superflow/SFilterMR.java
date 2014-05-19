package superflow;

import java.io.*;
import java.net.*;
import java.util.*;
import java.math.BigDecimal;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SFilterMR {
	
	static Path[] localFiles;
	static Set<String> white = new HashSet<String>();;
	static String ipset = "";
	static String stime = "", dur="", sip="", dip="", spkts="", dpkts="", sbytes="", dbytes="", sloss="", dloss="", sport = "", dport = ""; // 10 column
	
	public static class FilterMapper extends Mapper <LongWritable, Text, Text, Text> {
		
	    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
    		int dotPos = 0;
    		localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());

	        //Filter white-list	
    		BufferedReader whitelist = new BufferedReader(new FileReader(localFiles[0].toString()));
    		white = new HashSet<String>();
			while( whitelist.ready()){ 
				String whiteline = whitelist.readLine();
				white.add(whiteline);       //8.8.8.8  8.8.4.4  .......
			}
			whitelist.close();
			
			//Set domain ip
			BufferedReader domainlist = new BufferedReader(new FileReader(localFiles[1].toString()));
			if( domainlist.ready()){ 
				String tmpline = domainlist.readLine();
				String ip[] = tmpline.split(" ");
				if(ip[0].equals("host"))
					ipset = ip[1];  //140.116
			}
			domainlist.close();
		
		//Filter log into bipartite graph
		if (value.getLength() > 0) {
			String line = value.toString();
			String str[] = line.split("\\s+"); // 以空白分隔

			if(str.length == 10){
				try{
					stime = str[0]; 
					dur = str[1]; 
					dotPos = str[2].lastIndexOf(".");
					if ( dotPos >= 0 ) {
						sip = str[2].substring(0, dotPos);
						sport = str[2].substring( dotPos+1 );
					}
					dotPos = str[3].lastIndexOf(".");
					if ( dotPos >= 0 ) {
						dip = str[3].substring(0, dotPos);
						dport = str[3].substring( dotPos+1 );
					}
					spkts = str[4]; dpkts = str[5];
					sbytes = str[6]; dbytes = str[7];
					sloss = str[8]; dloss = str[9];
				
					//filter-out interconnect flows
					if( sip.indexOf(ipset) == 0 && dip.indexOf(ipset) < 0 ){  // ipset=140.116.164 sip:140.116.164.98=0->true  sip:130.140.116.164=5->false , 且dip 找不到140.116
						if(!white.contains(dip)){
							//stime, dur, sip, dip, spkts, dpkts, sbytes, dbytes, sloss, dloss
							if( Integer.parseInt(dip.substring(0, dip.indexOf("."))) < 224 && dip.indexOf("192.168") < 0 ){ 
								context.write(new Text(stime), new Text(sip + ":" + sport + ">" + dip + ":" + dport+","+dur+","+spkts+","+dpkts+","+sbytes+","+dbytes+","+sloss+","+dloss));
							}
						}
					}
					if( dip.indexOf(ipset) == 0 && sip.indexOf(ipset) < 0 ){
						if(!white.contains(sip)){
							if( Integer.parseInt(sip.substring(0, sip.indexOf("."))) < 224 && sip.indexOf("192.168") < 0 ){ 
								context.write(new Text(stime), new Text(sip + ":" + sport + ">" + dip + ":" + dport+","+dur+","+spkts+","+dpkts+","+sbytes+","+dbytes+","+sloss+","+dloss));					
							}
						}
					}
				}
				catch(Exception e){
					System.out.println("Exception format");
				}											
			}
		}
    }
}
	public static class FilterReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {

			BigDecimal spkts = new BigDecimal(0), dpkts = new BigDecimal(0), sbytes = new BigDecimal(0), dbytes = new BigDecimal(0), 
					   sloss = new BigDecimal(0), dloss = new BigDecimal(0);
			BigDecimal dur = new BigDecimal(0);
			BigDecimal failrate = new BigDecimal(0);
			BigDecimal failnum = new BigDecimal(0);
			BigDecimal totpkts = new BigDecimal(0);
			BigDecimal totbytes = new BigDecimal(0);
			
			//stime, dur, spkts, dpkts, sbytes, dbytes, sloss, dloss
			for( Text val: values){
				String col[] = val.toString().split(",");
				if(col.length == 8){
					col[1] = col[1].replace('*','0');
					dur = dur.add(new BigDecimal(col[1]));
					spkts = spkts.add(new BigDecimal(col[2]));
					dpkts = dpkts.add(new BigDecimal(col[3]));
					sbytes = sbytes.add(new BigDecimal(col[4]));
					dbytes = dbytes.add(new BigDecimal(col[5]));
					sloss = sloss.add(new BigDecimal(col[6]));
					dloss = dloss.add(new BigDecimal(col[7]));
				}
				failnum = sloss;
				
				try{
					failrate = sloss.divide(spkts,2,BigDecimal.ROUND_HALF_UP);
				}
				catch(Exception e){
					System.out.println("divide by zero");
				}
				
				totpkts = spkts.add(dpkts);
				totbytes = sbytes.add(dbytes);
				
				context.write(new Text("perFlow" + " " + key), new Text(col[0] + " IPN:" + dpkts + " OPN:" + spkts + " PPF:" + totpkts
						+ " TBF:" + dur + " IBN:"+ dbytes + " OBN:"+ sbytes + " TBT:"+ totbytes + " SLS:" + sloss + " DLS:" + dloss));
			}			
		}
	}
	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length < 2 ) {
			System.out.println("SuperFLow Filter: <in> <out>");
			System.exit(2);
		}
		
		DistributedCache.addCacheFile(new URI("/user/hpds/white"), conf);
		DistributedCache.addCacheFile(new URI("/user/hpds/domain"), conf);
		
		Job job = new Job(conf, "SFilter Stage"); 
		job.setJarByClass(SFilterMR.class);
		
		job.setMapperClass(FilterMapper.class);
		job.setReducerClass(FilterReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if ( job.waitForCompletion(true) ) {
			System.out.println("Parse argus format successful");
		}
	}
}
