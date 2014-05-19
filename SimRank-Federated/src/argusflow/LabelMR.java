package argusflow;

import java.io.*;
import java.math.BigDecimal;

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

public class LabelMR {
	
	static String groupID = "", pair = "", IPN="", OPN="", PPF="", TBF="", IBN="", OBN="", TBT="", FCN="", FCR="";   // 12 column

	public static class LabelMapper extends Mapper <LongWritable, Text, Text, Text> {
		
	    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
	    	
	    	//  1150_1	140.116.11.13>94.226.126.203 0 3 3 3.010230 0 194 194 2 0.67
			String line = value.toString();
			String[] str = line.split("\\s+"); // 以空白分割

			if(str.length == 11){
				groupID = str[0];
				pair = str[1];
				IPN = str[2];
				OPN = str[3];
				PPF = str[4];
				TBF = str[5];
				IBN = str[6];
				OBN = str[7];
				TBT = str[8];
				FCN = str[9];
				FCR = str[10];		
				context.write(new Text(groupID), new Text(pair + " " + IPN + " " + OPN + " " + PPF + " " + TBF + " " + IBN + " " + OBN + " " + TBT + " " + FCN + " " + FCR));
			}
		}	
	}
	public static class LabelReducer extends Reducer<Text, Text, Text, Text> {
		int group = 0;
		
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
			BigDecimal count = new BigDecimal(0);	
			
			StringBuilder sIP = new StringBuilder();
			StringBuilder dIP = new StringBuilder();
			sIP.append("SIP");
			dIP.append("DIP");
			
	        for(Text val : values){
	        	//context.write(key, val);
	        	String[] line = val.toString().split(" ");   // 140.116.11.13>94.54.2.144 0 3 3 3.010703 0 194 194 2 0.67	
	        	String pair = line[0];
	        	ipn = ipn.add(new BigDecimal(line[1]));
	        	opn = opn.add(new BigDecimal(line[2]));
	        	ppf = ppf.add(new BigDecimal(line[3]));
	        	tbf = tbf.add(new BigDecimal(line[4]));
	        	ibn = ibn.add(new BigDecimal(line[5]));
	        	obn = obn.add(new BigDecimal(line[6]));
	        	tbt = tbt.add(new BigDecimal(line[7]));     	
	        	fcn = fcn.add(new BigDecimal(line[8]));	 
	        	fcr = fcr.add(new BigDecimal(line[9]));
	        	count = count.add(new BigDecimal(1));
	        	
	        	String[] arr = pair.split(">");
	            sIP.append(":" + arr[0]);
	            dIP.append(":" + arr[1]);
	        }
	        group++;
	        
	        try{
	        	ipn = ipn.divide(count,2,BigDecimal.ROUND_HALF_UP);
	        	opn = opn.divide(count,2,BigDecimal.ROUND_HALF_UP);
	        	ppf = ppf.divide(count,2,BigDecimal.ROUND_HALF_UP);
	        	tbf = tbf.divide(count,2,BigDecimal.ROUND_HALF_UP);
	        	ibn = ibn.divide(count,2,BigDecimal.ROUND_HALF_UP);
	        	obn = obn.divide(count,2,BigDecimal.ROUND_HALF_UP);
	        	tbt = tbt.divide(count,2,BigDecimal.ROUND_HALF_UP);
	        	fcn = fcn.divide(count,2,BigDecimal.ROUND_HALF_UP);
	        	fcr = fcr.divide(count,2,BigDecimal.ROUND_HALF_UP);
	        }
	        catch(Exception e){
	        	System.out.println("divide by zero");
	        }
	        
        	context.write(new Text("GroupID-" + group), new Text(ipn + "," + opn + "," + ppf + "," + tbf + "," + ibn + "," + obn + "," + tbt + "," + fcn + "," + fcr 
        	+ "," + sIP.toString() +  "|" + dIP.toString()));
		}

	}
	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length < 2 ) {
			System.out.println("Label: <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Label"); 
		job.setJarByClass(LabelMR.class);
		
		job.setMapperClass(LabelMapper.class);
		job.setReducerClass(LabelReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if ( job.waitForCompletion(true) ) {
			System.out.println("Label group successful");
		}
	}
}
