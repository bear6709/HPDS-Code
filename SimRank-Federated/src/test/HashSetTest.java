package test;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class HashSetTest {
	public static void main(String[] args) throws ParseException {
		
		// �إߦ����ƶ��ؤ� int array
		int duplicateArray[] = { 4, 2, 5, 1, 5, 2, 4, 3 };
		
		// �Q�� Set ���S�ʡA�N�Ҧ����ة�J Set���Y�i�������ƪ�����  
		Set<Integer> intSet = new HashSet<Integer>();
		for (int element : duplicateArray) {
			intSet.add(element);
		}
		int nonDuplicateArray[] = new int[intSet.size()];
		
		// �N Set �������ب��X��� nonDuplicateArray ��    �]�i�H�Q�� iterator �ӹF��
		//Object[] tempArray = intSet.toArray();
		//for (int i = 0; i < tempArray.length; i++) {
		//	nonDuplicateArray[i] = (Integer) tempArray[i];
		//}
		
		// ��X���G�G1, 2, 3, 4, 5
		Iterator<Integer> iter = intSet.iterator();
		while(iter.hasNext()){
			System.out.println( iter.next() );
		}
		
	}	
}
