package org.hpds.multiedge.pagerank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

public class iplist {
	
	public static void main(String[] args) throws IOException {
		
		Scanner inp = new Scanner(System.in);
		System.out.print("file name: ");
		BufferedReader br = new BufferedReader( new FileReader("C:\\shared\\botnet\\ip\\"+inp.next()+".txt"));
		System.out.print("ip: ");
		String nd = inp.next();
		
		String desFile = "C:\\shared\\botnet\\ip\\ipfilterbot.txt";
		BufferedWriter bufWriter = new BufferedWriter(new FileWriter(desFile));
		
		String line;
		while((line = br.readLine()) != null){ 		
			if( line.indexOf(nd) >= 0){
				bufWriter.write(line);
				bufWriter.newLine();
			}
		}
		br.close();
		bufWriter.close();
		System.out.println("create ipfilter.txt");
	}
}
