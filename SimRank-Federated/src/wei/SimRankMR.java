package wei;

import java.io.IOException;
import java.util.Iterator;
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

public class SimRankMR {
	
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
			//計算兩筆feature之間的的關聯係數
			r = correlation(sgroup1[0], sgroup2[0]);
			
			//刪除重複的SIP
			Set<String> sip1 = new HashSet<String>();
			Set<String> sip2 = new HashSet<String>();
			String[] siplist1 = sgroup1[1].split(":");
			String[] siplist2 = sgroup2[1].split(":");
			for(String host1 : siplist1){
				sip1.add(host1);
			}
			for(String host2 : siplist2){
				sip2.add(host2);
			}
			Iterator<String> siter1 = sip1.iterator();
			StringBuilder sb1 = new StringBuilder();
	        sb1.append("SIP1");
			while(siter1.hasNext()){
				sb1.append(":"+siter1.next());
			}
			Iterator<String> siter2 = sip2.iterator();
			StringBuilder sb2 = new StringBuilder();
	        sb2.append("SIP2");
			while(siter2.hasNext()){
				sb2.append(":"+siter2.next());
			}
			
			//統計DIP的集合	
			String[] diplist1 = group1[1].split(":");
			String[] diplist2 = group2[1].split(":");
			
			Set<String> dip1 = new HashSet<String>();  //c,d,e belongs to A
			Set<String> dip2 = new HashSet<String>();  //d,f,g,h belongs to B
			
			for(String element1 : diplist1){
				if(!element1.equals("DIP"))
					dip1.add(element1);
			}
			for(String element2 : diplist2){
				if(!element2.equals("DIP"))
					dip2.add(element2);
			}
			
			//SimRank陣列 (n x m)運算
			Iterator<String> iter1 = dip1.iterator();
			while(iter1.hasNext()){
				String DIP1 = iter1.next();
				
				Iterator<String> iter2 = dip2.iterator();
				while(iter2.hasNext()){
					String DIP2 = iter2.next();
				
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
	public static class SimRankReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException {			
			
			double sum = 0;
			for(DoubleWritable val : values){
				sum += val.get();
			}
			context.write(key, new DoubleWritable(sum) );
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
		job.setJarByClass(SimRankMR.class);
		
		job.setMapperClass(SimRankMapper.class);
		job.setCombinerClass(SimRankCombiner.class); 
		job.setReducerClass(SimRankReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if ( job.waitForCompletion(true) ) {
			System.out.println("SimRank computation successful");
		}
	}
}
