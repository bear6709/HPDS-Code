package test;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class HashSetTest {
	public static void main(String[] args) throws ParseException {
		
		// 建立有重複項目之 int array
		int duplicateArray[] = { 4, 2, 5, 1, 5, 2, 4, 3 };
		
		// 利用 Set 的特性，將所有項目放入 Set中即可移除重複的項目  
		Set<Integer> intSet = new HashSet<Integer>();
		for (int element : duplicateArray) {
			intSet.add(element);
		}
		int nonDuplicateArray[] = new int[intSet.size()];
		
		// 將 Set 中的項目取出放到 nonDuplicateArray 中    也可以利用 iterator 來達成
		//Object[] tempArray = intSet.toArray();
		//for (int i = 0; i < tempArray.length; i++) {
		//	nonDuplicateArray[i] = (Integer) tempArray[i];
		//}
		
		// 輸出結果：1, 2, 3, 4, 5
		Iterator<Integer> iter = intSet.iterator();
		while(iter.hasNext()){
			System.out.println( iter.next() );
		}
		
	}	
}
