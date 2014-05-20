package superflow;

import java.io.IOException;
import java.util.ArrayList;

import object.StringIntPair;

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

public class GraphMR {
	
	public static class GraphMapper extends Mapper <LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {		
			
			String[] line = value.toString().split("\\s+");
			String[] token = line[0].split("-");

			context.write(new Text("pair"), new Text(token[1] + " " + line[1]));
		}
	}
	
	public static class GraphReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {			
			
			ArrayList<StringIntPair> group = new ArrayList<StringIntPair>();
			
			for(Text val : values){		
				String[] gid = val.toString().split(" ");
				int keyint = Integer.parseInt(gid[0]);
				group.add(new StringIntPair(keyint, gid[1]));   // 1    0.00,1.00,1.00,..............
			}
			
			//任取兩個不同group建立連線
			for(StringIntPair g : group){
				for(StringIntPair g2 : group){
					if(g.getKey() < g2.getKey()){					
						context.write(new Text("s("+g.getKey()+","+g2.getKey()+")"), new Text( g.getValue() + ";" + g2.getValue() ));
					}
				}
			}
		}
	}			
	
	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length < 2 ) {
			System.out.println("Buildind Edge: <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Graph"); 
		job.setJarByClass(GraphMR.class);
		
		job.setMapperClass(GraphMapper.class);
		job.setReducerClass(GraphReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if ( job.waitForCompletion(true) ) {
			System.out.println("Construct Graph successful");
		}
	}
}
