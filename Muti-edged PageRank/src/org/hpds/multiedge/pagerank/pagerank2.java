package org.hpds.multiedge.pagerank;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

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

public class pagerank2 {
	
	static int flag = 0;
	static Path[] localFiles;
	public static String result = "false";
	
	
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
		static Boolean firstloop = false;
		static Map<String,String> graph;
		
		@SuppressWarnings("deprecation")

		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		    try { 
                doMap(key,value,context);
            } catch ( Exception e ) {
                e.printStackTrace();
            }    
        }
		public void doMap(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			//0_140.116.164.71        :[1,1,1]:0_140.116.164.91[0,14,0]|0_140.116.164.97[0,9,0]
			if( flag == 0){
				
				FileSystem dfs = FileSystem.get(context.getConfiguration());
				StringBuffer outputstring = new StringBuffer(context.getConfiguration().get("outputstring"));
				StringBuffer filename = new StringBuffer();
				int filenum = 0;
				for(int i = outputstring.length()-1; i >=0; i--){
					if( outputstring.charAt(i) > 57 || outputstring.charAt(i) < 48 ){
						filename.append(outputstring.substring(0, i + 1));
						filenum = new Integer( outputstring.substring(i + 1));
						break;
					}
				}				
				if( filenum == 1){
					firstloop = true;
					//outputstring.replace( "pr", "gr");
				}else{
					filename.append( filenum - 1);								 					
					Path path = new Path( filename.toString());
					FileStatus[] files = dfs.listStatus(path);
					
					graph = new HashMap<String,String>();
					for (int i = 0; i < files.length; i++) {
						if (!files[i].isDir()) {
							FSDataInputStream is = dfs.open(files[i].getPath());
	                        String line = "";
	                        while ((line = is.readLine()) != null) {	
	                        	String tmpline[] = line.split(":");
	                        	String tmpline2[] = tmpline[0].split("	");
								graph.put( tmpline2[0], tmpline[1]);
	                        }
						}
					}	
				}
				
				flag = 1;
			}
			
			String line[] = value.toString().split(":");
			
			if( line.length >= 3){	
				
				String score = "";
				String flowid = "";
				if( firstloop){
					score = line[1];
				}else{
					String tmpline[] = line[0].split("	");
					flowid = tmpline[0];
					score = graph.get(flowid);
				}
				
				StringTokenizer vElement = new StringTokenizer( line[2], "|");		//split edge
				
				@SuppressWarnings("unchecked")
				Map<String,String> list[] = new Map[3];	
				for( int i = 0; i < list.length; i++)
					list[i] = new HashMap<String, String>();
				
				String max[] = {"0","0","0"};		//l,s,t
				String sum[] = {"0","0","0"};
				
				while (vElement.hasMoreTokens()) {
					
					String ip[] = vElement.nextToken().split("\\[");		//0,14,0]
					String val[] = ip[1].substring( 0, ip[1].length() - 1).split(",");		//feature value: 0 14 0
					
					for( int i = 0; i < list.length; i++){
						if( !val[i].equals("-1")){		//have feature edge
							if( new BigDecimal( val[i]).compareTo( new BigDecimal( max[i])) == 1)		//find the max feature value
								max[i] = val[i];
							
							sum[i] = String.valueOf( new BigDecimal( sum[i]).add( new BigDecimal(val[i])));		//(max1 - f1) + (max1 - f2) = 2 * max1 - (f1 + f2) -> find f1 + f2
							list[i].put( ip[0], val[i]);
						}
					}
				}			

				String rank[] = score.substring( 1, score.length() - 1).split(",");		//[1,1,1] -> 1 1 1
				
				for( int i = 0; i <= 2; i++){		//for each feature
					
					BigDecimal current_score = new BigDecimal( rank[i]);
					BigDecimal maxf = new BigDecimal( max[i]); 
					String[] rs = (String[])list[i].keySet().toArray(new String[0]);
					BigDecimal divisor = maxf.multiply( new BigDecimal( rs.length)).subtract( new BigDecimal( sum[i])).add( new BigDecimal( rs.length));		//n * max1 - (f1 + f2 + ... + fn) + n
					
					for( String val : rs){
						BigDecimal dividend;
						BigDecimal result;
						if( !list[i].get(val).equals("-1")){		//have feature edge
							dividend = maxf.subtract( new BigDecimal( list[i].get(val))).add( new BigDecimal(1));		//max - f1 + 1
							result = current_score.multiply( dividend.divide( divisor, 5, BigDecimal.ROUND_HALF_UP));		//rank * dividend/divisor	
							if( i == 0)
								context.write( new Text(val), new Text( "i_" + result.toString()));		//0_140.116.164.91 , i+rank
							else if( i == 1)
								context.write( new Text(val), new Text( "r_" + result.toString()));
							else
								context.write( new Text(val), new Text( "t_" + result.toString()));
						}
					}
				}
			}			
			
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
            try {
                doReduce(key,values,context);
            } catch (Exception e ) {
                e.printStackTrace();
            }
        }
		public void doReduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
		/*	for (Text val : values) {
				context.write( key, val);
			}*/
			
			
			if( flag == 0){
				localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				BufferedReader dnslist = new BufferedReader( new FileReader(localFiles[0].toString()));
				while( dnslist.ready()){ 
					String tmpline = dnslist.readLine();
					String ip[] = tmpline.split(" ");
					if(ip[0].equals("end"))
						result = ip[1];
				}
				flag = 1;
			}
			
			BigDecimal sumi = new BigDecimal(0);
			BigDecimal sumr = new BigDecimal(0);
			BigDecimal sumt = new BigDecimal(0);
			
			for (Text val : values) {
				
				String line[] = val.toString().split("_");
				BigDecimal tmp = new BigDecimal( line[1].toString());
				
				if( line[0].equals("i"))
					sumi = sumi.add(tmp);
				else if( line[0].equals("r"))
					sumr = sumr.add(tmp);
				else if( line[0].equals("t"))
					sumt = sumt.add(tmp);				
			}
			
			sumi = sumi.setScale(3, BigDecimal.ROUND_HALF_UP);
			sumr = sumr.setScale(3, BigDecimal.ROUND_HALF_UP);
			sumt = sumt.setScale(3, BigDecimal.ROUND_HALF_UP);
			
			double entropy = 0.0;
			BigDecimal zero = new BigDecimal(0);
			if( result.equals("true")){
				
				if( sumi.compareTo( zero) == 0)
					sumi = new BigDecimal(1);
				if( sumr.compareTo( zero) == 0)
					sumr = new BigDecimal(1);
				if( sumt.compareTo( zero) == 0)
					sumt = new BigDecimal(1);
				
				entropy = sumi.doubleValue() * (Math.log(sumi.doubleValue())/Math.log(3)) + sumr.doubleValue() * (Math.log(sumr.doubleValue())/Math.log(3)) + sumt.doubleValue() * (Math.log(sumt.doubleValue())/Math.log(3));
				StringBuffer output = new StringBuffer();
				output.append(sumi).append(",").append(sumr).append(",").append(sumt).append(",");
				context.write( key, new Text( String.format( output + "%.3f", entropy)));
			}else{
				StringBuffer output = new StringBuffer();
				output.append(":[").append(sumi).append(",").append(sumr).append(",").append(sumt).append("]:");
				context.write( key, new Text(output.toString()));
			}
	
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Pagerank <in> <out>");
			System.exit(2);
		}
		
		DistributedCache.addCacheFile(new URI("/user/hpds/config"),conf);		
		conf.set("outputstring", args[1]);
		
		FileSystem fs = FileSystem.get( conf);
		if( fs.exists(new Path("/user/hpds/featurelist")))
			fs.deleteOnExit( new Path("/user/hpds/featurelist"));		//remove featurelist temp file
		
		Job job = new Job(conf, "pagerank2");

		job.setJarByClass(pagerank2.class);
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
