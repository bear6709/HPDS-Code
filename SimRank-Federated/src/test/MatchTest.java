package test;

public class MatchTest {
	public static void main(String[] args){
		String a = "10.2.0.5";
	    String b = "124.41.12.87.5053";
		
	    //String strRegExp = "(\d{1,2}|1 \d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])";
	    
	    if(a.split("\\.").length == 4 ){
	    	System.out.println("No port");
		}
	    else if(a.split("\\.").length == 5){
	    	System.out.println("Has port");
	    }
	    
	}
}
