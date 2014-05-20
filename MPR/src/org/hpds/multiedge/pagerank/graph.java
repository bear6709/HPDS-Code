package org.hpds.multiedge.pagerank;

import java.io.IOException;

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

import org.hpds.multiedge.feature.graph_list;

public class graph {
    private static graph_list graphList = new graph_list();	
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			//l0_140.116.164.71	length/packet: 41  
			if( value.getLength() > 0){
				String line = value.toString();
				String ip[] = line.split("_");
				
				context.write( new Text(ip[0]), new Text(line));		//(l0 , 140.116.164.71	length/packet: 41)
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
            try {
                switch( key.charAt(0)){
                    case 's':				
                        graphList.smallpacket( key, values, context);
                        break;
                    case 't': 
                        graphList.packetpermin(  key, values, context);
                        break;

                    case 'l':
                        graphList.avalength(  key, values, context);
                        break;	
                    default:
                        break;
                }	
            } catch ( Exception e ) {
                e.printStackTrace();
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
		
		/*
        FileSystem fs = FileSystem.get( conf);
		if( fs.exists(new Path("/user/hpds/stime")))
			fs.deleteOnExit( new Path("/user/hpds/stime"));		//remove timestamp temp file
        */
	
		
		Job job = new Job(conf, "graph");

		job.setJarByClass(graph.class);
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
