package test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class SIPtest {
	public static void main(String[] args){
		
		String a = "140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.11.2:140.116.164.86:140.116.164.86:140.116.164.86";
		String b = "140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.11.13:140.116.11.1:140.116.11.86";
		
		HashMap<String, Integer> sip1 = new HashMap<String, Integer>();  //累加不重複SIP的次數 
		HashMap<String, Integer> sip2 = new HashMap<String, Integer>();  
		
		ArrayList<String> sadd1 = new ArrayList<String>(); //存放重複key的SIP
		ArrayList<String> sadd2 = new ArrayList<String>(); 
		
		Set<String> s1 = new HashSet<String>(); //存放不重複key的SIP
		Set<String> s2 = new HashSet<String>();
		
		String[] siplist1 = a.split(":");
		String[] siplist2 = b.split(":");
		for(String host1 : siplist1){
			s1.add(host1);
			sadd1.add(host1);
		}
		for(String host2 : siplist2){
			s2.add(host2);
			sadd2.add(host2);
		}
	
		for(Object key : s1){
			int count1 = 0;
			for(Object key1 : sadd1){
				if(key.equals(key1)){
					count1++;
					sip1.put(key.toString(), count1);
				}
			}
		}
		for (Object key : sip1.keySet()) {
			System.out.println(key + "(" + sip1.get(key) + ")");
		}
		
		for(Object key : s2){
			int count2 = 0;
			for(Object key2 : sadd2){
				if(key.equals(key2)){
					count2++;
					sip2.put(key.toString(), count2);
				}
			}
		}
		for (Object key : sip2.keySet()) {
			System.out.println(key + "(" + sip2.get(key) + ")");
		}
	}
}
