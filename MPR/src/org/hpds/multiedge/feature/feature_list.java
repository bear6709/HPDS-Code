package org.hpds.multiedge.feature;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.hpds.multiedge.pagerank.feature;

public class feature_list extends Reducer<Text, Text, Text, Text>  {
	//static String stime = "16:17:26.697304";
	public String stime = feature.stime;
	public long tinterval = Long.parseLong(feature.tinterval) * 60;		//if feature.tinterval=15mins tinterval=900s
	
	public void smallpacket(Text key, Map<String,String> list, String[] rs, Context context)throws IOException, InterruptedException {		//s
	
		SimpleDateFormat tf = new SimpleDateFormat("HH:mm:ss");
		long snum = 0, lnum = 0;	
		try{
			Date d1 = tf.parse(rs[0]);		//d1 timestamp = first log timestamp
			Date st = tf.parse(stime);
			long interval = ( ( d1.getTime() - st.getTime()) / 1000) / tinterval;		//first log in which interval 
			Date d2 = null;
			long d3 = st.getTime() + ( tinterval * 1000) * ( interval + 1);		//next time interval
			
			for( int i = 0; i < rs.length; i++) {
				if( i < rs.length - 1)		//if not the last log
					d2 = tf.parse(rs[i + 1]);		//d2 = next log timestamp
				else
					d2 = tf.parse(rs[i]);
				
				if( list.get(rs[i]).equals("small"))
					snum++;
				else
					lnum++;
				
				if( d2.getTime() >= d3){
					d1 = d2;
					BigDecimal s = new BigDecimal(snum);
					BigDecimal l = new BigDecimal(lnum);
					BigDecimal ratio = s.divide(l.add(s), 2, BigDecimal.ROUND_HALF_UP).multiply( new BigDecimal(100));
					String strs = String.format("snum: %-6d  lnum: %-6d  ratio: %-6s", snum, lnum, String.valueOf(ratio.intValue())+"%");
					String outkey = key.toString().charAt(0) + String.valueOf(interval) + "_" + key.toString().substring(1, key.toString().length());		//s interval _ ip
					context.write( new Text(outkey), new Text( strs));
					snum = 0;
					lnum = 0;
					interval = ( ( d1.getTime() - st.getTime()) / 1000) / tinterval;
					d3 = st.getTime() + ( tinterval * 1000) * ( interval + 1);
				}
			}
			if( snum != 0 || lnum != 0){
				BigDecimal s = new BigDecimal(snum);
				BigDecimal l = new BigDecimal(lnum);
				BigDecimal ratio = s.divide(l.add(s), 2, BigDecimal.ROUND_HALF_UP).multiply( new BigDecimal(100));
				String strs = String.format("snum: %-6d  lnum: %-6d  ratio: %-6s", snum, lnum, String.valueOf(ratio.intValue())+"%");
				String outkey = key.toString().charAt(0) + String.valueOf(interval) + "_" + key.toString().substring(1, key.toString().length());
				context.write( new Text(outkey), new Text( strs));
			}
		}catch (ParseException e){
		    e.printStackTrace();
		}
	}
	
	public void packetpersec(Text key, Map<String,String> list, String[] rs, Context context)throws IOException, InterruptedException {		//t
		
		SimpleDateFormat tf = new SimpleDateFormat("HH:mm:ss");
		long num = 0;
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
				
				num++;
				
				if( d2.getTime() >= d3){
					BigDecimal s = new BigDecimal(num);
					BigDecimal l = new BigDecimal(tinterval);
					BigDecimal packetps = s.divide(l, 4, BigDecimal.ROUND_HALF_UP);
					//String strt = String.format("num: %-6d  packets/sec: %-6s", num, String.valueOf(packetps));
					
					String strt = String.format("num: %-6d  tin: %-6s  packets/sec: %-6s", num, String.valueOf(l), String.valueOf(packetps));
					String outkey = key.toString().charAt(0) + String.valueOf(interval) + "_" + key.toString().substring(1, key.toString().length());		//t interval _ ip
					
					//String strt = String.valueOf(packetps);
					//String outkey = "flow	packetpersec	" + String.valueOf(interval) + "	" + key.toString().substring(1, key.toString().length());
					
					context.write( new Text(outkey), new Text( strt));
					d1 = d2;
					num = 0;
					interval = ( ( d1.getTime() - st.getTime()) / 1000) / tinterval;
					d3 = st.getTime() + ( tinterval * 1000) * ( interval + 1);
				}
			}
			//if( d2.getTime() - d1.getTime() > 0){
			if( num > 0){
				BigDecimal s = new BigDecimal(num);
				BigDecimal l = new BigDecimal(tinterval);
				BigDecimal packetps = s.divide(l, 4, BigDecimal.ROUND_HALF_UP);
				//String strt = String.format("num: %-6d  packets/sec: %-6s", num, String.valueOf(packetps));
				
				String strt = String.format("num: %-6d  ltin: %-6s  packets/sec: %-6s", num, String.valueOf(l), String.valueOf(packetps));
				String outkey = key.toString().charAt(0) + String.valueOf(interval) + "_" + key.toString().substring(1, key.toString().length());
				
				//String strt = String.valueOf(packetps);
				//String outkey = "flow	packetpersec	" + String.valueOf(interval) + "	" + key.toString().substring(1, key.toString().length());
				
				context.write( new Text(outkey), new Text( strt));
			}
		}catch (ParseException e){
		    e.printStackTrace();
		}
	}
	
	public void avglength(Text key, Map<String,String> list, String[] rs, Context context)throws IOException, InterruptedException {		//l
		
		SimpleDateFormat tf = new SimpleDateFormat("HH:mm:ss");
		long totall = 0, countlog = 0;
		String strl = null;
		try{
			Date d1 = tf.parse(rs[0]);
			Date st = tf.parse(stime);
			long interval = ( ( d1.getTime() - st.getTime()) / 1000) / tinterval;
			Date d2 = null;
			long d3 = st.getTime() + ( tinterval* 1000) * ( interval + 1);
			
			for( int i = 0; i < rs.length; i++) {
				
				if( i < rs.length - 1)
					d2 = tf.parse(rs[i + 1]);
				else
					d2 = tf.parse(rs[i]);
				
				totall += Long.parseLong( list.get(rs[i]));
				countlog++;
				
				if( d2.getTime() >= d3){
					d1 = d2;
					long lps = totall / countlog;
					//strl = String.format("avg: %-6d  counts: %-6d  countlog: %-6d  length/packet: %-6d  time: %-6s", avg, counts, countlog, lps, d2);
					strl = String.format("length(Byte)/packet: %-6d", lps);
					String outkey = key.toString().charAt(0) + String.valueOf(interval) + "_" + key.toString().substring(1, key.toString().length());		//l interval _ ip
					context.write( new Text(outkey), new Text( strl));
					totall = 0; 
					countlog = 0;
					interval = ( ( d1.getTime() - st.getTime()) / 1000) / tinterval;
					d3 = st.getTime() + ( tinterval* 1000) * ( interval + 1);
				}			
			}
			
			if( countlog > 0){
				long lps = totall / countlog;
				//strl = String.format("avg: %-6d  counts: %-6d  countlog: %-6d  length/packet: %-6d  time: %-6s", avg, counts, countlog, lps, d2);
				strl = String.format("length(Byte)/packet: %-6d", lps);
				String outkey = key.toString().charAt(0) + String.valueOf(interval) + "_" + key.toString().substring(1, key.toString().length());
				context.write( new Text(outkey), new Text( strl));
			}
		}catch (ParseException e){
		    e.printStackTrace();
		}
	}
	
	public void ioratio(Text key, Map<String,String> list, String[] rs, Context context)throws IOException, InterruptedException {		//r
		
		SimpleDateFormat tf = new SimpleDateFormat("HH:mm:ss");
		long inum = 0, onum = 0;	
		try{
			Date d1 = tf.parse(rs[0]);		//d1 timestamp = first log timestamp
			Date st = tf.parse(stime);
			long interval = ( ( d1.getTime() - st.getTime()) / 1000) / tinterval;		//first log in which interval 
			Date d2 = null;
			long d3 = st.getTime() + ( tinterval * 1000) * ( interval + 1);		//next time interval
			
			for( int i = 0; i < rs.length; i++) {
				if( i < rs.length - 1)		//if not the last log
					d2 = tf.parse(rs[i + 1]);		//d2 = next log timestamp
				else
					d2 = tf.parse(rs[i]);
				
				if( list.get(rs[i]).equals("incoming"))
					inum++;
				else
					onum++;
				
				if( d2.getTime() >= d3){
					d1 = d2;
					BigDecimal s = new BigDecimal(inum);
					BigDecimal l = new BigDecimal(onum);
					BigDecimal ratio = s.divide(l.add(s), 2, BigDecimal.ROUND_HALF_UP).multiply( new BigDecimal(100));
					String rtrs = String.format("inum: %-6d  onum: %-6d  ratio: %-6s", inum, onum, String.valueOf(ratio.intValue())+"%");
					String outkey = key.toString().charAt(0) + String.valueOf(interval) + "_" + key.toString().substring(1, key.toString().length());		//s interval _ ip
					
					//String rtrs = String.valueOf(ratio.intValue()) + "%";
					//String outkey = "flow	ioratio	" + String.valueOf(interval) + "	" + key.toString().substring(1, key.toString().length());		//s interval _ ip
					
					context.write( new Text(outkey), new Text( rtrs));
					inum = 0;
					onum = 0;
					interval = ( ( d1.getTime() - st.getTime()) / 1000) / tinterval;
					d3 = st.getTime() + ( tinterval * 1000) * ( interval + 1);
				}
			}
			if( inum != 0 || onum != 0){
				BigDecimal s = new BigDecimal(inum);
				BigDecimal l = new BigDecimal(onum);
				BigDecimal ratio = s.divide(l.add(s), 2, BigDecimal.ROUND_HALF_UP).multiply( new BigDecimal(100));
				String rtrs = String.format("inum: %-6d  onum: %-6d  ratio: %-6s", inum, onum, String.valueOf(ratio.intValue())+"%");
				String outkey = key.toString().charAt(0) + String.valueOf(interval) + "_" + key.toString().substring(1, key.toString().length());
				
				//String rtrs = String.valueOf(ratio.intValue()) + "%";
				//String outkey = "flow	ioratio	" + String.valueOf(interval) + "	" + key.toString().substring(1, key.toString().length());		//s interval _ ip
				
				context.write( new Text(outkey), new Text( rtrs));
			}
		}catch (ParseException e){
		    e.printStackTrace();
		}
	}
	
	public void avglengthperflow(Text key, Map<String,String> list, String[] rs, Context context)throws IOException, InterruptedException {		//f
		
		SimpleDateFormat tf = new SimpleDateFormat("HH:mm:ss");
		long totall = 0, countlog = 0;
		String strf = null;
		try{
			Date d1 = tf.parse(rs[0]);
			Date st = tf.parse(stime);
			long interval = ( ( d1.getTime() - st.getTime()) / 1000) / tinterval;
			Date d2 = null;
			long d3 = st.getTime() + ( tinterval* 1000) * ( interval + 1);
			
			for( int i = 0; i < rs.length; i++) {			
				if( i < rs.length - 1)
					d2 = tf.parse(rs[i + 1]);
				else
					d2 = tf.parse(rs[i]);
				
				totall += Long.parseLong( list.get(rs[i]));
				countlog++;
				
				if( d2.getTime() >= d3){
					d1 = d2;
					long lps = totall / countlog;
					//strl = String.format("avg: %-6d  counts: %-6d  countlog: %-6d  length/packet: %-6d  time: %-6s", avg, counts, countlog, lps, d2);
					strf = String.format("length(Byte)/flow: %-6d", lps);
					String outkey = key.toString().charAt(0) + String.valueOf(interval) + "_" + key.toString().substring(1, key.toString().length());		//l interval _ ip
					context.write( new Text(outkey), new Text( strf));
					totall = 0; 
					countlog = 0;
					interval = ( ( d1.getTime() - st.getTime()) / 1000) / tinterval;
					d3 = st.getTime() + ( tinterval* 1000) * ( interval + 1);
				}			
			}
			
			if( countlog > 0){
				long lps = totall / countlog;
				//strl = String.format("avg: %-6d  counts: %-6d  countlog: %-6d  length/packet: %-6d  time: %-6s", avg, counts, countlog, lps, d2);
				strf = String.format("length(Byte)/flow: %-6d", lps);
				String outkey = key.toString().charAt(0) + String.valueOf(interval) + "_" + key.toString().substring(1, key.toString().length());
				context.write( new Text(outkey), new Text( strf));
			}
		}catch (ParseException e){
		    e.printStackTrace();
		}
	}
	
	public void nullpacketnum(Text key, Map<String,String> list, String[] rs, Context context)throws IOException, InterruptedException {		//n
		
		SimpleDateFormat tf = new SimpleDateFormat("HH:mm:ss");
		long num = 0;
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
				
				num++;
				
				if( d2.getTime() >= d3){
					String ntrt = String.format("number of nullpacket: %-6s", String.valueOf(num));
					String outkey = key.toString().charAt(0) + String.valueOf(interval) + "_" + key.toString().substring(1, key.toString().length());		//t interval _ ip
					context.write( new Text(outkey), new Text( ntrt));
					d1 = d2;
					num = 0;
					interval = ( ( d1.getTime() - st.getTime()) / 1000) / tinterval;
					d3 = st.getTime() + ( tinterval * 1000) * ( interval + 1);
				}
			}
			if( d2.getTime() - d1.getTime() > 0){
				String ntrt = String.format("number of nullpacket: %-6s", String.valueOf(num));
				String outkey = key.toString().charAt(0) + String.valueOf(interval) + "_" + key.toString().substring(1, key.toString().length());
				context.write( new Text(outkey), new Text( ntrt));
			}
		}catch (ParseException e){
		    e.printStackTrace();
		}
	}
	
	public void standarddeviation(Text key, Map<String,String> list, String[] rs, Context context)throws IOException, InterruptedException {		//d

		SimpleDateFormat tf = new SimpleDateFormat("HH:mm:ss");
		long totall = 0, countlog = 0, squares = 0;
		String strd = null;
		try{
			Date d1 = tf.parse(rs[0]);
			Date st = tf.parse(stime);
			long interval = ( ( d1.getTime() - st.getTime()) / 1000) / tinterval;
			Date d2 = null;
			long d3 = st.getTime() + ( tinterval* 1000) * ( interval + 1);
			
			for( int i = 0; i < rs.length; i++) {
				
				if( i < rs.length - 1)
					d2 = tf.parse(rs[i + 1]);
				else
					d2 = tf.parse(rs[i]);
				
				long length = Long.parseLong( list.get(rs[i]));
				totall += length;
				squares += length * length;
				countlog++;
				
				if( d2.getTime() >= d3){
					d1 = d2;
					BigDecimal quareofaverage = new BigDecimal(squares).divide(new BigDecimal(countlog), 2, BigDecimal.ROUND_HALF_UP);
					BigDecimal avg = new BigDecimal(totall).divide(new BigDecimal(countlog), 2, BigDecimal.ROUND_HALF_UP);
					BigDecimal veragesquare = avg.multiply(avg);
					BigDecimal sd =  quareofaverage.subtract(veragesquare);
					strd = String.format("Standard Deviation of packet length: %-6.2f", Math.pow(sd.doubleValue(), 0.5));
					//Math.pow(sd.doubleValue(),0.5);
					String outkey = key.toString().charAt(0) + String.valueOf(interval) + "_" + key.toString().substring(1, key.toString().length());		//l interval _ ip
					context.write( new Text(outkey), new Text( strd));
					totall = 0; 
					countlog = 0;
					squares = 0;
					interval = ( ( d1.getTime() - st.getTime()) / 1000) / tinterval;
					d3 = st.getTime() + ( tinterval* 1000) * ( interval + 1);
				}			
			}
			if( countlog > 0){
				BigDecimal quareofaverage = new BigDecimal(squares).divide(new BigDecimal(countlog), 2, BigDecimal.ROUND_HALF_UP);
				BigDecimal avg = new BigDecimal(totall).divide(new BigDecimal(countlog), 2, BigDecimal.ROUND_HALF_UP);
				BigDecimal veragesquare = avg.multiply(avg);
				BigDecimal sd =  quareofaverage.subtract(veragesquare);
				strd = String.format("Standard Deviation of packet length: %-6.2f", Math.pow(sd.doubleValue(), 0.5));
				String outkey = key.toString().charAt(0) + String.valueOf(interval) + "_" + key.toString().substring(1, key.toString().length());
				context.write( new Text(outkey), new Text( strd));
			}
		}catch (ParseException e){
		    e.printStackTrace();
		}
	}
	
	public void avginterstitial(Text key, Map<String,String> list, String[] rs, Context context)throws IOException, InterruptedException {		//i
		
		SimpleDateFormat tf = new SimpleDateFormat("HH:mm:ss");
		long totall = 0, countinterstitial = 0;
		String stri = null;
		try{
			Date d1 = tf.parse(rs[0]);
			Date st = tf.parse(stime);
			long interval = ( ( d1.getTime() - st.getTime()) / 1000) / tinterval;
			Date d2 = null;
			long d3 = st.getTime() + ( tinterval* 1000) * ( interval + 1);
			boolean log = false;
			
			for( int i = 0; i < rs.length; i++) {
				log = true;
				
				if( i < rs.length - 1)
					d2 = tf.parse(rs[i + 1]);
				else
					d2 = tf.parse(rs[i]);
					
				if(  d2.getTime() < d3 && d2 != tf.parse(rs[i])){
					totall += ( ( d2.getTime() - tf.parse(rs[i]).getTime()) / 1000);
					countinterstitial++;
				}
				else{
					d1 = d2;
					BigDecimal ait = new BigDecimal(tinterval);
					if( countinterstitial > 0)
						ait = new BigDecimal(totall).divide( new BigDecimal(countinterstitial), 2, BigDecimal.ROUND_HALF_UP);
					//long ait = avg / countinterstitial;
					stri = String.format("%d, %d, avg interstitial time: %-6s", totall, countinterstitial, ait);
					String outkey = key.toString().charAt(0) + String.valueOf(interval) + "_" + key.toString().substring(1, key.toString().length());		//l interval _ ip
					
					//stri = ait.toString();
					//String outkey = "flow	avginterstitial	" + String.valueOf(interval) + "	" + key.toString().substring(1, key.toString().length());		//l interval _ ip
					
					context.write( new Text(outkey), new Text( stri));
					totall = 0; 
					countinterstitial = 0;
					log = false;
					interval = ( ( d1.getTime() - st.getTime()) / 1000) / tinterval;
					d3 = st.getTime() + ( tinterval* 1000) * ( interval + 1);
				}			
			}
			if( log == true){
				BigDecimal ait = new BigDecimal(tinterval);
				if( countinterstitial > 0)
					ait = new BigDecimal(totall).divide( new BigDecimal(countinterstitial), 2, BigDecimal.ROUND_HALF_UP);
				stri = String.format("%d, %d, avg interstitial time: %-6s", totall, countinterstitial, ait);
				String outkey = key.toString().charAt(0) + String.valueOf(interval) + "_" + key.toString().substring(1, key.toString().length());
				
				//stri = ait.toString();
				//String outkey = "flow	avginterstitial	" + String.valueOf(interval) + "	" + key.toString().substring(1, key.toString().length());		//l interval _ ip
				
				context.write( new Text(outkey), new Text( stri));
			}
			
		}catch (ParseException e){
		    e.printStackTrace();
		}
	}
}
