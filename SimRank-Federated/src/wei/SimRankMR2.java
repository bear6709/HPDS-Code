package wei;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SimRankMR2 {
	
	public static class SimRankMapper extends Mapper <LongWritable, Text, Text, DoubleWritable> {
		
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {		
			double r = 0;
			String[] line = value.toString().split("\\s+");
			String[] group = line[1].split(";");
			
			String[] group1 = group[0].split("\\|");
			String[] group2 = group[1].split("\\|");
			
			String[] sgroup1 = group1[0].split(",SIP:");
			String[] sgroup2 = group2[0].split(",SIP:");
			
			//sgroup[0] = feature list , sgroup[1] = SIP list
			
			//計算兩筆feature之間的的Pearson關聯係數
			r = correlation(sgroup1[0], sgroup2[0]);
			
			//統計重複的SIP
			HashMap<String, Integer> sip1 = new HashMap<String, Integer>();  //累加不重複SIP的次數 
			HashMap<String, Integer> sip2 = new HashMap<String, Integer>();  
			
			ArrayList<String> sadd1 = new ArrayList<String>(); //存放重複key的SIP
			ArrayList<String> sadd2 = new ArrayList<String>(); 
			
			Set<String> s1 = new HashSet<String>(); //存放不重複key的SIP
			Set<String> s2 = new HashSet<String>();
			
			String[] siplist1 = sgroup1[1].split(":");
			String[] siplist2 = sgroup2[1].split(":");
			
			for(String host1 : siplist1){
				s1.add(host1);
				sadd1.add(host1);
			}
			for(String host2 : siplist2){
				s2.add(host2);
				sadd2.add(host2);
			}
		
			for(Object key0 : s1){
				int count1 = 0;
				for(Object key1 : sadd1){
					if(key0.equals(key1)){
						count1++;
						sip1.put(key0.toString(), count1);
					}
				}
			}	
			for(Object key2 : s2){
				int count2 = 0;
				for(Object key3 : sadd2){
					if(key2.equals(key3)){
						count2++;
						sip2.put(key2.toString(), count2);
					}
				}
			}
			
			//印出SIP list
			StringBuilder sb1 = new StringBuilder();
	        sb1.append("SIP1");
	        for (Object SIP1 : sip1.keySet()) {
				sb1.append(":" + SIP1 + "(" + sip1.get(SIP1) + ")");
			}
			StringBuilder sb2 = new StringBuilder();
	        sb2.append("SIP2");
	        for (Object SIP2 : sip2.keySet()) {
				sb2.append(":" + SIP2 + "(" + sip2.get(SIP2) + ")");
			}
			
			//建立DIP的集合	
			String[] diplist1 = group1[1].split(":");
			String[] diplist2 = group2[1].split(":");
			
			Set<String> dip1 = new HashSet<String>();  //c,d,e belong to A
			Set<String> dip2 = new HashSet<String>();  //d,f,g,h belong to B
			
			for(String element1 : diplist1){
				if(!element1.equals("DIP"))
					dip1.add(element1);
			}
			for(String element2 : diplist2){
				if(!element2.equals("DIP"))
					dip2.add(element2);
			}
			
			//SimRank陣列 (n x m)運算
			for(String DIP1 : dip1){
				for(String DIP2 : dip2){			
					if(DIP1.equals(DIP2)){
						context.write(new Text( line[0] + ":[" + sb1 + "][" + sb2 + "]"), new DoubleWritable(r) );
					}
					else{
						context.write(new Text( line[0] + ":[" + sb1 + "][" + sb2 + "]"), new DoubleWritable(0) );
					}
				}
			}			
		}
		
		public double correlation(String f1, String f2) {
			double r = 0;
			double sum1 = 0, avg1 = 0;
			double sum2 = 0, avg2 = 0;
			double top = 0;
			double bottom = 0;
			double x = 0, y = 0;
			
			String[] feature1 = f1.split(",");
			String[] feature2 = f2.split(",");
			
			for(String element1 : feature1){
				sum1 += Double.parseDouble(element1);
			}
			avg1 = sum1 / feature1.length;
			
			for(String element2 : feature2){
				sum2 += Double.parseDouble(element2);
			}
			avg2 = sum2 / feature2.length;
			
			for(int i = 0; i < 9; i++){
				top += (Double.parseDouble(feature1[i])-avg1) * (Double.parseDouble(feature2[i])-avg2);				
			}
			for(int j = 0; j < 9; j++){
				x += Math.pow(Double.parseDouble(feature1[j])-avg1, 2);
				y += Math.pow(Double.parseDouble(feature2[j])-avg2, 2);
			}
			bottom = Math.pow(x, 0.5) * Math.pow(y, 0.5);
			
			try{
				r = Math.abs(top/bottom);
			}catch(Exception e){
				System.out.println("partition by zero");
			}
			
			return r;
		}
	}
	public static class SimRankReducer extends Reducer<Text, DoubleWritable, DoubleWritable, Text> {
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException {			
			
			double sum = 0;
			for(DoubleWritable val : values){
				sum += val.get();
			}
			context.write(new DoubleWritable(sum), key);
		}
	}			
	
	public static class SimRankCombiner extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {  
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			for(DoubleWritable val : values){
				sum += val.get();
			}
			context.write(key, new DoubleWritable(sum) );
		}
	}
	
	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length < 2 ) {
			System.out.println("SimRank: <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "SimRank"); 
		job.setJarByClass(SimRankMR2.class);
		
		job.setMapperClass(SimRankMapper.class);
		job.setCombinerClass(SimRankCombiner.class); 
		job.setReducerClass(SimRankReducer.class);

		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if ( job.waitForCompletion(true) ) {
			System.out.println("SimRank computation successful");
		}
	}
}
