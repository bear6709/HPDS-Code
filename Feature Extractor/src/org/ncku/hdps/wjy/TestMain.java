package org.ncku.hdps.wjy;

public class TestMain {
	  public static void main(String[] args) throws Exception {
		  String test1 = "08:14:42.348516 ARP, Request who-has 140.116.164.125 tell 140.116.164.85, length 28";
		  String test2 = "08:14:43.310493 IP 140.116.164.86.17500 > 140.116.164.127.17500: UDP, length 119";
		  String test3 = "08:14:43.343047 IP 69.171.246.16.80 > 140.116.164.97.55546: Flags [P.], seq 2226618919:2226619686, ack 1131943935, win 64240, length 767";
		  FlowFormat ff = new FlowFormat(test1,true);
		  FlowFormat ff2 = new FlowFormat();
		  System.out.println("test1 parsing result " + ff);
		  ff.parse(test2, true);
		  System.out.println("test2 parsing result " + ff);
		  ff.parse(test3, true);
		  System.out.println("test3 parsing result " + ff);
		  ff2.parse(ff.toString(), false); 
		  System.out.println("ff2 parsing with ff result " + ff);
	  }
}
