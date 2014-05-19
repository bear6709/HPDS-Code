package test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class CorrelationTest {

	public static void main(String[] args){
		String value = "s(2027,2025)	0.00,3.00,3.00,3.05,0.00,194.00,194.00,2.00,0.67,SIP:140.116.11.10:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86|DIP:14.207.40.127:27.20.201.143:27.38.56.171:37.46.58.1:46.249.90.173:58.250.213.196:59.54.69.0:60.48.48.198:60.48.48.66:61.131.13.186:61.154.185.23:61.60.216.215;0.00,2.00,2.00,3.00,0.00,132.00,132.00,1.00,0.50,SIP:140.116.11.12:140.116.11.13:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86:140.116.164.86|DIP:14.207.40.127:27.20.201.143:27.38.56.171:37.46.58.1:46.249.90.173:58.250.213.196:59.54.69.0:60.219.214.39:60.25.10.100:60.48.48.104:61.131.11.157:72.198.91.180:14.216.245.144:1.164.111.80:1.164.225.82";
		 							  //0.00,2.00,2.00,3.00,0.00,132.00,132.00,1.00,0.50
		
		//DIP:66.61.117.141:1.34.216.158:14.109.115.128:14.136.187.186:27.155.230.153:27.20.33.26:49.48.214.17:49.52.25.221:58.212.118.44:58.55.208.38:59.45.71.135:60.0.71.245:60.48.48.118:60.48.48.191:60.48.48.198:60.48.48.66:61.131.13.186:61.154.185.23:61.60.216.215;
		//DIP:14.207.40.127:27.20.201.143:27.38.56.171:37.46.58.1:46.249.90.173:58.250.213.196:59.54.69.0:60.219.214.39:60.25.10.100:60.48.48.104:61.131.11.157:72.198.91.180:14.216.245.144:1.164.111.80:1.164.225.82";
		
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
					System.out.println(line[0] + ":[" + sb1 + "][" + sb2 + "]" );
					System.out.println(r);
				}
				else{
					System.out.println(line[0] + ":[" + sb1 + "][" + sb2 + "]" );
					System.out.println(0);
				}
			}
		}
	
	}
	public static double correlation(String f1, String f2) {
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
