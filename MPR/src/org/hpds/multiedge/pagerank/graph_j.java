//pagerank initial score: rescale

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

import org.hpds.multiedge.feature.AdjacencyList;
import org.hpds.multiedge.feature.NodeContainer;

public class graph_j {
	
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
		static int flag = 0;
		static Path[] localFiles;
		static BigDecimal avginterstitial = new BigDecimal(0);
		static BigDecimal ioratio = new BigDecimal(0);
		static BigDecimal packetpersec = new BigDecimal(0);
		static Map<String,List<String>> feature;
		static BigDecimal []maxfeature = {new BigDecimal(0),new BigDecimal(0),new BigDecimal(0)};
		static BigDecimal newscale = new BigDecimal(100);
		
		private List<NodeContainer> mLogList = new ArrayList<NodeContainer>();
		//private List<AdjacencyList> mAdjacencyMatrix = new LinkedList<AdjacencyList>();

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
					
					mLogList.add(new NodeContainer(line));
					
				/*	int index = line.indexOf("_");
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
					}*/
				}
				
				flag = 1;
			}			
			
			if( value.getLength() > 0){		//ex: 0_140.116.164.83.5173_111.255.1.215.56931	4.62,49,0.2122
				
				float tmpAvgIntervalDiff = 0.0f;
				float tmpIORatioDiff = 0.0f;
				float tmpPacketPerSecDiff = 0.0f;
				boolean bAvgIntervalEdge = false; // 1
				boolean bIORatioEdge = false; // 5 
				boolean bPacketPerSecEdge = false; // 0.002
				//PrintStream pOut = new PrintStream(  new FileOutputStream(outFilePath) );
				AdjacencyList alist = new AdjacencyList();
				//double comp_time = (double)(mLogList.size())/1000.0f;
				
				alist.clear();
				NodeContainer ncI = new NodeContainer(value.toString());
				alist.setNode(ncI);
				
			/*	if ( (i+1) % 1000 == 0 ) {
					double x1 = (double)(i+1) / 1000.0f;
					
					System.out.println("process Node " + i + " total comp_time " +  x1 * comp_time + " M \tncI = " + ncI);
				}*/
				for ( int j = 0 ; j < mLogList.size(); j++ ) {
					NodeContainer ncJ = mLogList.get(j);
					tmpAvgIntervalDiff = 0.0f;
					tmpIORatioDiff = 0.0f;
					tmpPacketPerSecDiff = 0.0f;
					bAvgIntervalEdge = false;
					bIORatioEdge = false;
					bPacketPerSecEdge = false;
					if ( ncI.getNodeID().equals(ncJ.getNodeID()) == false ) {
						if ( ncI.getAvgInterval() > 0.0f && ncJ.getAvgInterval() > 0.0f) {
							tmpAvgIntervalDiff = Math.abs(ncI.getAvgInterval() - ncJ.getAvgInterval());
						} else {
							tmpAvgIntervalDiff = -1.0f;
						}
						if ( ncI.getIORatio() > 0.0f && ncJ.getIORatio() > 0.0f ) {
							tmpIORatioDiff = Math.abs(ncI.getIORatio() - ncJ.getIORatio());
						} else {
							tmpIORatioDiff = -1.0f;
						}
						if ( ncI.getPacketPerSec() > 0.0f && ncJ.getPacketPerSec() > 0.0f ) {
							tmpPacketPerSecDiff = Math.abs(ncI.getPacketPerSec()- ncJ.getPacketPerSec());
						} else {
							tmpPacketPerSecDiff = -1.0f;
						}
									
						if ( tmpAvgIntervalDiff <= 1.0f ) {
							bAvgIntervalEdge = true;
							if ( tmpAvgIntervalDiff <= 0.0f ) 
								tmpAvgIntervalDiff = 1.0f; // give minimal value if diff equals zero
						} else {
							tmpAvgIntervalDiff = -1.0f;
						}
						if ( tmpIORatioDiff <= 5.0f ) {
							bIORatioEdge = true;
							if ( tmpIORatioDiff <= 0.0f ) 
								tmpIORatioDiff = 1.0f; // give minimal value if diff equals zero
						} else {
							tmpIORatioDiff = -1.0f;
						}
						if ( tmpPacketPerSecDiff <= 0.002 ) {
							bPacketPerSecEdge = true;
							if ( tmpPacketPerSecDiff <= 0.0f ) 
								tmpPacketPerSecDiff = 0.001f; // give minimal value if diff equals zero
						} else {
							tmpPacketPerSecDiff = -1.0f;
						}
						if ( (bAvgIntervalEdge && bIORatioEdge) || (bIORatioEdge && bPacketPerSecEdge) || (bAvgIntervalEdge && bPacketPerSecEdge)  ) {
							alist.addAdjacencyNode(ncJ);
						}
					}
				}
				if ( alist.getAdjacencyNodeSize() > 0 ) {
					//String result = value.toString();
					//String output[] = result.split("	");
					context.write( new Text(""), new Text(alist.toString()));
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
		DistributedCache.addCacheFile(new URI(args[0]),conf);

		Job job = new Job(conf, "graph_j");
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
