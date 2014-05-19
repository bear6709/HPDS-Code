package object;

public class StringIntPair {
	int gID;
	String gVector;
	
	public StringIntPair(int groupID, String groupVector){
		this.gID = groupID;
		this.gVector = groupVector;
	}
	
	public int getKey(){
		return gID;
	}
	
	public String getValue(){
		return gVector;
	}
}
