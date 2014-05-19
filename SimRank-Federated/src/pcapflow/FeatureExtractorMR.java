package pcapflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class FeatureExtractorMR {

	public static class FeatureExtractorMapper 
		extends Mapper<LongWritable, Text, LongWritable, Text>{
		private static LongWritable mOutKey = new LongWritable();
		private static Text mOutValue = new Text();
		//private static FlowFormat mFF = new FlowFormat();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			FlowFormat mFF = new FlowFormat();
			mFF.parse(value.toString(), true);
			mOutKey.set(mFF.getKeyHash());   // 
			mOutValue.set(mFF.toString());   // Time Proto SIP SPort DIP Dport Type Length
			// filter out unused data
			if ( mFF.getSrcIP().equals("none") == false && mFF.getLength() >= 0 ) {
				context.write(mOutKey, mOutValue);
			}
		}
	}
	public static class FeatureExtractorReducer 
		extends Reducer<LongWritable,Text,Text,Text> {
		private static FlowFormat mFF = new FlowFormat();
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<FlowCollector> list = new ArrayList<FlowCollector>();
			FlowCollector fc = null;
			for ( Text text : values) { // hash(srcIP) + hash(dstIP) 
				mFF.parse(text.toString(), false);
				if ( list.size() <= 0 ) {
					list.add(new FlowCollector(mFF));
				}
				//-------------------------------------------
				// to handle the collision of hashing IP's pair
				fc = null;
				for ( FlowCollector collector : list ) {
					if ( collector.isIPMatch(mFF) ) {
						fc = collector;
						break;
					}
				}
				if ( fc == null ) {
					fc = new FlowCollector(mFF);
					list.add(fc);
				}
				//--------------------------------------------
				fc.addFlow(mFF);
			}
			for ( FlowCollector collector : list ) {
				collector.weighAllFeatures();
				collector.outputResult(context);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 2 ) {
			System.out.println("<in> <out>");
			System.exit(2);
		}
	
		Job job = Job.getInstance(conf, "Flow Feature Extractor"); 

		job.setJarByClass(FeatureExtractorMR.class);
		job.setMapperClass(FeatureExtractorMapper.class);
		job.setReducerClass(FeatureExtractorReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.out.println("input : " + otherArgs[0] );
		System.out.println("output : " + otherArgs[1] );

		if ( job.waitForCompletion(true) ) {
			System.out.println("parse PCAP format successful");
		}
	}
}
