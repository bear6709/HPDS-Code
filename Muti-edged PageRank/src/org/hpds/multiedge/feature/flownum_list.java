package org.hpds.multiedge.feature;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.hpds.multiedge.pagerank.flownum;

public class flownum_list extends Reducer<Text, Text, Text, Text>  {
	//static String stime = "16:17:26.697304";
	public String stime = flownum.stime;
	public long tinterval = Long.parseLong(flownum.tinterval) * 60;		//if feature.tinterval=15mins tinterval=900s
	
	public void packetpersec(Text key, Map<String,String> list, String[] rs, Context context)throws IOException, InterruptedException {		//t
		
		SimpleDateFormat tf = new SimpleDateFormat("HH:mm:ss");
		//long num = 0;
		try{
			Date d1 = tf.parse(rs[0]);
			Date st = tf.parse(stime);
			long interval = ( ( d1.getTime() - st.getTime()) / 1000) / tinterval;
			Date d2 = null;
			long d3 = st.getTime() + ( tinterval * 1000) * ( interval + 1);
			
			for( int i = 0; i < rs.length; i++) {
				if( i < rs.length - 1)
					d2 = tf.parse(rs[i + 1]);
				else
					d2 = tf.parse(rs[i]);
				
			
				
				if( d2.getTime() >= d3){
					
					String outkey = String.valueOf(interval) + "_" + key.toString().substring(1, key.toString().length());		//t interval _ ip
					
					//String strt = String.valueOf(packetps);
					//String outkey = "flow	packetpersec	" + String.valueOf(interval) + "	" + key.toString().substring(1, key.toString().length());
					
					context.write( new Text(outkey), new Text("test"));
					d1 = d2;
					interval = ( ( d1.getTime() - st.getTime()) / 1000) / tinterval;
					d3 = st.getTime() + ( tinterval * 1000) * ( interval + 1);
				}
			}
			//if( d2.getTime() - d1.getTime() > 0){
			
				
				String outkey = String.valueOf(interval) + "_" + key.toString().substring(1, key.toString().length());
				
				//String strt = String.valueOf(packetps);
				//String outkey = "flow	packetpersec	" + String.valueOf(interval) + "	" + key.toString().substring(1, key.toString().length());
				
				context.write( new Text(outkey), new Text("test"));
			
		}catch (ParseException e){
		    e.printStackTrace();
		}
	}
	
	
}
