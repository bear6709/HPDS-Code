package test;

import java.util.HashMap;

import object.StringIntPair;

public class ArrayListTest {
	public static void main(String[] args) {
		HashMap<String, StringIntPair> map1 = new HashMap<String, StringIntPair>();
		HashMap<String, StringIntPair> map2 = new HashMap<String, StringIntPair>();
		
		map1.put("A", new StringIntPair(201, "123456"));
		map1.put("B", new StringIntPair(102, "4567896"));
		
		
		map2.put("A", new StringIntPair(3345678, "777777"));
		
		for (Object key1 : map1.keySet()) {
                        
            for(Object key2 : map2.keySet()) {
            	if(key1.equals(key2)){
            		System.out.println(key1 + "=" + key2 + " : " + map1.get(key1).getKey() + "  " + map2.get(key2).getKey() );
            	}
            }
        }
		
	}
}
