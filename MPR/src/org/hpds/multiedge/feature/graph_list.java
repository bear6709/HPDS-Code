package org.hpds.multiedge.feature;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class graph_list extends Reducer<Text, Text, Text, Text> {
	
	public void smallpacket(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
		Map<String,String> list = new HashMap<String,String>();	
		for( Text val : values) {
			String line[] = val.toString().split("	");		//140.116.164.71	snum: 4643    lnum: 19460   ratio: 19% 
			String ip = line[0];
			String score = line[1].substring( line[1].indexOf("ratio:") + 6, line[1].length()).trim();		//19%
			score = score.substring( 0, score.length() - 1);		//19
			list.put( ip, score);
		}
		String[] rs = (String[])list.keySet().toArray(new String[0]);
		
		for( String sip : rs){		//every IP pairs
			String out = sip + ":";
			for( String dip : rs){
				if( sip != dip){
					long sub = Long.parseLong(list.get(sip)) - Long.parseLong(list.get(dip));		
					if( ( sub <= 10 && sub >= 0) || ( sub <= 0 && sub >= -10)){
						out = out + dip + "[" + list.get(dip) + "],";
					}
				}
			}
			if( out.charAt(out.length() - 1) == ',')
				out = out.substring( 0, out.length() - 1);
			context.write( new Text(sip), new Text(out));
		}
	}
	
	public void packetpermin(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
		Map<String,String> list = new HashMap<String,String>();	
		for( Text val : values) {
			String line[] = val.toString().split("	");		//140.116.164.71	packets/sec: 0.27
			String ip = line[0];
			String score = line[1].substring( line[1].indexOf(":") + 1, line[1].length()).trim();
			list.put( ip, score);
		}
		String[] rs = (String[])list.keySet().toArray(new String[0]);
		
		for( String sip : rs){
			String out = sip + ":";
			for( String dip : rs){
				if( sip != dip){
					BigDecimal a = new BigDecimal(list.get(sip));
					BigDecimal b = new BigDecimal(list.get(dip));
					if( a.subtract(b).abs().compareTo(new BigDecimal(100)) == -1){
						out = out + dip + "[" + list.get(dip) + "],";
					}
				}
			}
			if( out.charAt(out.length() - 1) == ',')
				out = out.substring( 0, out.length() - 1);
			context.write( new Text(sip), new Text(out));
		}
	}
	
	public void avalength(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
		Map<String,String> list = new HashMap<String,String>();	
		for( Text val : values) {
			String line[] = val.toString().split("	");		//140.116.164.71	length/packet: 41 
			String ip = line[0];
			String score = line[1].substring( line[1].indexOf(":") + 1, line[1].length()).trim();
			list.put( ip, score);
		}
		String[] rs = (String[])list.keySet().toArray(new String[0]);
		
		for( String sip : rs){
			String out = sip + ":";
			for( String dip : rs){
				if( sip != dip){
					long sub = Long.parseLong(list.get(sip)) - Long.parseLong(list.get(dip));
					if( ( sub <= 200 && sub >= 0) || ( sub <= 0 && sub >= -200)){
						out = out + dip + "[" + list.get(dip) + "],";
					}
				}
			}
			if( out.charAt(out.length() - 1) == ',')
				out = out.substring( 0, out.length() - 1);
			context.write( new Text(sip), new Text(out));
		}
	}

}
