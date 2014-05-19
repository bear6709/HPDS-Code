package argusflow;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import object.DataObject;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GroupMR {  //do nomal flow group
	
	static String stime = "", pair = "", IPN="", OPN="", PPF="", TBF="", IBN="", OBN="", TBT="", FCN="", FCR="";   // 12 column
	static String ipn="",opn="",ppf="",tbf="",ibn="",obn="",tbt="",fcn="",fcr="";
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
			
			// perFlow 00:00:00.075329	140.116.164.86:56882>60.48.48.228:9408 IPN:0 OPN:2 PPF:2 TBF:2.997453 IBN:0 OBN:132 TBT:132 FCN:1 FCR:0.50
			if(str.length == 12){
				stime = str[1];  
				String[] IPpair = str[2].split(">");
				String[] host1 = IPpair[0].split(":");
				String[] host2 = IPpair[1].split(":");
				pair = host1[0] + ">" + host2[0];
				
				IPN = str[3]; OPN = str[4]; PPF = str[5]; 
				TBF = str[6];
				IBN = str[7]; OBN = str[8]; TBT = str[9];
				FCN = str[10]; FCR = str[11];
				
				String[] arr = IPN.split(":");
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
				fcn = arr[1];
				arr = FCR.split(":");
				fcr = arr[1];
				
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
				
				context.write(new Text(String.valueOf(interval)), new Text(pair + " " + ipn + " " + opn + " " + ppf + " " + tbf + " " + ibn + " " + obn + " " + tbt + " " + fcn + " " + fcr));
				// 0	140.116.164.86>60.48.48.228 0 2 2 2.997453 0 132 132 1 0.50
			}
		}	
	}
	public static class GroupReducer extends Reducer<Text, Text, Text, Text> {
	    double Eps = 0.01;   //半徑
	    int MinPts = 15;   //密度
		
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {			
			
			ArrayList<DataObject> list = new ArrayList<DataObject>();
			
	        for(Text val : values){
	        	String[] line = val.toString().split(" ");       	
	        	
	        	DataObject oneflow = new DataObject();
	        	oneflow.setFlow(line[0],line[1],line[2],line[3],line[4],line[5],line[6],line[7],line[8],line[9]);
	        	list.add(oneflow);
	        }
	        
			Iterator<DataObject> iter = list.iterator();
            
			int clusterID=0;
			
			//iteration of p, which is visited
            while(iter.hasNext()){
                DataObject p=iter.next();
                if(p.isVisited())
                    continue;
                p.setVisited(true);     //設為visited後就已經確定了它是核心點還是邊界點
                
                // getNeighbors of p
                Vector<DataObject> neighbors = getNeighbors(p, list);
                
                // expand cluster of p
                if(neighbors.size()<MinPts){
                    if(p.getCid()<=0)
                        p.setCid(-1);       //cid初始為0,表示未分類；分類後設置為一個正數；設置為-1表示noise
                }else{
                    if(p.getCid()<=0){
                        clusterID++;
                        expandCluster(p,neighbors,clusterID,list);
                    }else{
                        int iid=p.getCid();
                        expandCluster(p,neighbors,iid,list);
                    }
                }
                
                if(neighbors.size() > MinPts){
                	for(DataObject q : neighbors){
                		if(q.getCid() > 0 ){
                			context.write(new Text(key + "_" + q.getCid()),new Text(q.getVector()));
                		}
                	}
                }                
            }
		}
		
		public void expandCluster(DataObject p, Vector<DataObject> neighbors, int clusterID,ArrayList<DataObject> objects) {
	        p.setCid(clusterID);
	        Iterator<DataObject> iter=neighbors.iterator();
	        while(iter.hasNext()){
	            DataObject q=iter.next();
	            if(!q.isVisited()){
	                q.setVisited(true);
	                Vector<DataObject> qneighbors=getNeighbors(q,objects);
	                if(qneighbors.size()>=MinPts){
	                    Iterator<DataObject> it=qneighbors.iterator();
	                    while(it.hasNext()){
	                        DataObject no=it.next();
	                        if(no.getCid()<=0)
	                            no.setCid(clusterID);
	                    }
	                }
	            }
	            if(q.getCid()<=0){       //q不屬於任何群的成員
	                q.setCid(clusterID);
	            }
	        }
	    } 
		
	    public Vector<DataObject> getNeighbors(DataObject p,ArrayList<DataObject> objects){
	        Vector<DataObject> neighbors=new Vector<DataObject>();
	        Iterator<DataObject> iter=objects.iterator();
	        while(iter.hasNext()){
	            DataObject q = iter.next();
	            String arr1 = p.getVector();
	            String arr2 = q.getVector();
	            
	            String[] a = arr1.split(" ");
	            String[] b = arr2.split(" ");
	            
	            double distance = 0;
	            double sum = 0; 
	            
	            for(int i = 1; i < 10; i++){ 
	            	distance = Math.pow(Double.parseDouble(a[i])-Double.parseDouble(b[i]), 2);
	            	sum += distance;            	
	            }
	            if( Math.pow(sum, 0.5) <= Eps){      //計算距離
	        		neighbors.add(q);
	        	}
	        }
	        return neighbors;
	    }		
	}
	
	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length < 2 ) {
			System.out.println("Group: <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Super Group"); 
		job.setJarByClass(GroupMR.class);
		
		job.setMapperClass(GroupMapper.class);
		job.setReducerClass(GroupReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
	
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if ( job.waitForCompletion(true) ) {
			System.out.println("Group successful");
		}
	}
}
