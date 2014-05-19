package superflow;

import java.io.*;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SFlowMR {  //do superflow group
	
	static String stime = "", pair = "", IPN="", OPN="", PPF="", TBF="", IBN="", OBN="", TBT="", FCN="", FCR="";   // 12 column
	static String ipn="",opn="",ppf="",tbf="",ibn="",obn="",tbt="",sls="",dls="";
	static SimpleDateFormat tf = new SimpleDateFormat("HH:mm:ss");
	static SimpleDateFormat sf = new SimpleDateFormat("HH:mm:ss");
	static Date time = new Date();
	static Date tos = new Date();
	static String st = "00:00:00"; 
	static long timeInMilliseconds = 0;
	static long stimeInMilliseconds = 0;
	static long tinterval = 0;
	static int interval = 0;
    
	public static class GroupMapper extends Mapper <LongWritable, Text, Text, Text> {
		
	    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {

			String line = value.toString();
			String[] str = line.split("\\s+"); // 以空白分隔

			if(str.length == 12){
				
				stime = str[1]; 
				
				//140.116.11.12:64046>79.114.87.215:21315 IPN:0 OPN:1 PPF:1 TBF:0.000000 IBN:0 OBN:62 TBT:62 SLS:1 DLS:1 
				pair = str[2]; 
				String[] arr = pair.split(">");
				String[] sarr = arr[0].split(":");
				String[] darr = arr[1].split(":");
				
				IPN = str[3]; OPN = str[4]; PPF = str[5]; 
				TBF = str[6];
				IBN = str[7]; OBN = str[8]; TBT = str[9];
				FCN = str[10]; FCR = str[11];
				
				arr = IPN.split(":");
				ipn = arr[1];
				arr = OPN.split(":");
				opn = arr[1];
				arr = PPF.split(":");
				ppf = arr[1];
				arr = TBF.split(":");
				tbf = arr[1];
				arr = IBN.split(":");
				ibn = arr[1];
				arr = OBN.split(":");
				obn = arr[1];
				arr = TBT.split(":");
				tbt = arr[1];
				arr = FCN.split(":");
				sls = arr[1];
				arr = FCR.split(":");
				dls = arr[1];
				
				try{
					time = tf.parse(stime);
					tos = sf.parse(st);
					
					timeInMilliseconds = time.getTime();
					stimeInMilliseconds = tos.getTime();
				} 
				catch ( Exception e ) {
			       	e.printStackTrace();
			    }
				tinterval = timeInMilliseconds - stimeInMilliseconds;
				interval = (int) tinterval/60000;   // 1分鐘為一個時間間隔 = 60000毫秒
				
				context.write(new Text(interval + "_" + sarr[0]+">"+darr[0]), new Text(ipn + " " + opn + " " + ppf + " " + tbf + " " + ibn + " " + obn + " " + tbt + " " + sls + " " + dls));
			}
		}	
	}
	public static class GroupReducer extends Reducer<Text, Text, Text, Text> {
		
	    double Eps = 0.01;   //半徑
	    int MinPts = 15;   //密度
		
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {			
			
			BigDecimal ipn = new BigDecimal(0);
			BigDecimal opn = new BigDecimal(0);
			BigDecimal ppf = new BigDecimal(0);
			BigDecimal tbf = new BigDecimal(0);
			BigDecimal ibn = new BigDecimal(0);
			BigDecimal obn = new BigDecimal(0);
			BigDecimal tbt = new BigDecimal(0);
			BigDecimal fcn = new BigDecimal(0);
			BigDecimal fcr = new BigDecimal(0);			
			
			String[] pair = key.toString().split("_");   // 0_140.116.164.86:56882>60.48.48.228:9408
			
	        for(Text val : values){
	        	//context.write(key, val);
	        	String[] line = val.toString().split(" ");   // IPN:0 OPN:2 PPF:2 TBF:2.997453 IBN:0 OBN:132 TBT:132 SLS:1 DLS:0	        		        	
	        	ipn = ipn.add(new BigDecimal(line[0]));
	        	opn = opn.add(new BigDecimal(line[1]));
	        	ppf = ppf.add(new BigDecimal(line[2]));
	        	tbf = tbf.add(new BigDecimal(line[3]));
	        	ibn = ibn.add(new BigDecimal(line[4]));
	        	obn = obn.add(new BigDecimal(line[5]));
	        	tbt = tbt.add(new BigDecimal(line[6]));     	
	        	//context.write( key, new Text(oneflow.getVector()) );
	        	fcn = fcn.add(new BigDecimal(line[7]));	        	
	        }
	        
	        try{
	        	fcr = fcn.divide(opn,2,BigDecimal.ROUND_HALF_UP);
	        }
	        catch(Exception e){
				System.out.println("divide by zero");
			}
	        
	        if( fcr.doubleValue() >= 0.5){
	        	context.write(new Text("perFlow" + " " + pair[0]),new Text( pair[1]+ " IPN:" + ipn + " OPN:" + opn + " PPF:" + ppf
					+ " TBF:" + tbf + " IBN:"+ ibn + " OBN:"+ obn + " TBT:"+ tbt + " FCN:" + fcn + " FCR:" + fcr));
	        }
		}

	}
	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length < 2 ) {
			System.out.println("SuperFlow: <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Super Flow"); 
		job.setJarByClass(SFlowMR.class);
		
		job.setMapperClass(GroupMapper.class);
		job.setReducerClass(GroupReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if ( job.waitForCompletion(true) ) {
			System.out.println("SF Transform successful");
		}
	}
}
