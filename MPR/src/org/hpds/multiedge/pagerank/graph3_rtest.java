//pagerank initial score: rescale
//StringBuffer
package org.hpds.multiedge.pagerank;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

public class graph3_rtest {
	
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
		static int flag = 0;
		static Path[] localFiles;
		static BigDecimal avginterstitial = new BigDecimal(0);
		static BigDecimal ioratio = new BigDecimal(0);
		static BigDecimal packetpersec = new BigDecimal(0);
		static Map<String,List<String>> feature;
		static BigDecimal []maxfeature = {new BigDecimal(0),new BigDecimal(0),new BigDecimal(0)};
		static BigDecimal newscale = new BigDecimal(100);

		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			
			if( flag == 0){
				localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				BufferedReader list = new BufferedReader( new FileReader(localFiles[0].toString()));
				while( list.ready()){ 
					String tmpline = list.readLine();
					String ip[] = tmpline.split(" ");
					if(ip[0].equals("avginterstitial"))
						avginterstitial = new BigDecimal(ip[1]);
					if(ip[0].equals("ioratio"))
						ioratio = new BigDecimal(ip[1]);
					if(ip[0].equals("packetpersec"))
						packetpersec = new BigDecimal(ip[1]);
				}
				
				list = new BufferedReader( new FileReader(localFiles[1].toString()));
				feature = new HashMap<String,List<String>>();		//time interval 2-array				
				
				while( list.ready()){ 
					String line = list.readLine();
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
					
					String line2[] = line.split("	");
					String feval[] = line2[1].split(",");
					for( int i = 0; i <= 2; i++){
						if( new BigDecimal(feval[i]).compareTo(maxfeature[i]) == 1)
							maxfeature[i] = new BigDecimal(feval[i]);
					}
				}
				
				flag = 1;
			}			
			
			if( value.getLength() > 0){		//ex: 0_140.116.164.83.5173_111.255.1.215.56931	4.62,49,0.2122
				String line[] = value.toString().split("	");
				String sip = line[0].substring( line[0].indexOf("_") + 1);		//140.116.164.83.5173_111.255.1.215.56931					
				String score[] = line[1].split(",");		//4.62,49,0.2122
				
				BigDecimal [] rescale = {new BigDecimal(0),new BigDecimal(0),new BigDecimal(0)};
				for( int i = 0; i <= 2; i++){
					rescale[i] = newscale.multiply( new BigDecimal(score[i])).divide( maxfeature[i], 3, BigDecimal.ROUND_HALF_UP);
				}
				StringBuffer result = new StringBuffer( ":[" + rescale[0] + "," + rescale[1] + "," + rescale[2] + "]:");
				//String result = ":[" + rescale[0] + "," + rescale[1] + "," + rescale[2] + "]:";
				
				String timeslot = line[0].substring( 0, line[0].indexOf("_"));
				
				String[] rs = (String[])feature.get(timeslot).toArray(new String[0]);
				for( String val : rs){
					
					//if( val.substring( 0, val.indexOf("_")).equals( line[0].substring( 0, line[0].indexOf("_")))){		//if in the same time interval
					String fline[] = val.toString().split("	");
					String dip = fline[0].substring( fline[0].indexOf("_") + 1, fline[0].length());
						
					if( !sip.equals(dip)){
						String fscore[] = fline[1].split(",");						
						StringBuffer temp = new StringBuffer("");
							
						if( !score[0].equals("-1") && !fscore[0].equals("-1")){		//i
							BigDecimal a = new BigDecimal(score[0]);
							BigDecimal b = new BigDecimal(fscore[0]);
							if( a.subtract(b).abs().compareTo(avginterstitial) == -1){
								temp.append("[").append( a.subtract(b).abs()).append( ",");
							}else
								temp.append("[-1,");
						}else
							temp.append("[-1,");
							
						if( !score[1].equals("-1") && !fscore[1].equals("-1")){		//r
							BigDecimal a = new BigDecimal(score[1]);
							BigDecimal b = new BigDecimal(fscore[1]);
							if( a.subtract(b).abs().compareTo(ioratio) == -1){
								temp.append(a.subtract(b).abs()).append(",");
							}else
								temp.append("-1,");
						}else
							temp.append("-1,");
						
						if( !score[2].equals("-1") && !fscore[2].equals("-1")){		//t
							BigDecimal a = new BigDecimal(score[2]);
							BigDecimal b = new BigDecimal(fscore[2]);
							if( a.subtract(b).abs().compareTo(packetpersec) == -1){
								temp.append(a.subtract(b).abs()).append("]|");
							}else
								temp.append("-1]|");
						}else
							temp.append("-1]|");
							
						if( !temp.toString().equals("[-1,-1,-1]|"))		//have feature edge
							result.append(fline[0]).append(temp);
						
					}
				}
				if( result.charAt(result.length() - 1) == '|')
					result.deleteCharAt( result.length() - 1);
	
				context.write( new Text(line[0]), new Text(result.toString()));
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
		DistributedCache.addCacheFile(new URI("/user/hpds/featurelist"),conf);

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
