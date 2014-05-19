package object;
import org.apache.hadoop.io.Text;

public class DataObject extends Text{
	boolean visit = false;
	int cid = 0;
	//String pair = "none", IPN="none", OPN="none", PPF="none", TBF="none", IBN="none", OBN="none", TBT="none", FCN="none", FCR="none";
	String pair, IPN, OPN, PPF, TBF, IBN, OBN, TBT, FCN, FCR;
	String gid, address;
	
	public DataObject(){
		setPair("none");
		setIPN("none");
		setOPN("none");
		setPPF("none");
		setTBF("none");
		setIBN("none");
		setOBN("none");
		setTBT("none");
		setFCN("none");
		setFCR("none");
	}
	
	public DataObject(String pair, String ipn, String opn, String ppf, String tbf, String ibn, String obn, String tbt, String fcn, String fcr){
		setPair(pair);
		setIPN(ipn);
		setOPN(opn);
		setPPF(ppf);
		setTBF(tbf);
		setIBN(ibn);
		setOBN(obn);
		setTBT(tbt);
		setFCN(fcn);
		setFCR(fcr);
	}
	
	public void setFlow(String pair, String ipn, String opn, String ppf, String tbf, String ibn, String obn, String tbt, String fcn, String fcr){
		this.pair = pair;
		this.IPN = ipn;
		this.OPN = opn;
		this.PPF = ppf;
		this.TBF = tbf;
		this.IBN = ibn;
		this.OBN = obn;
		this.TBT = tbt;
		this.FCN = fcn;
		this.FCR = fcr;
	}
	public void setGroup(String gid, String ipn, String opn, String ppf, String tbf, String ibn, String obn, String tbt, String fcn, String fcr, String address){
		this.gid = gid;
		this.IPN = ipn;
		this.OPN = opn;
		this.PPF = ppf;
		this.TBF = tbf;
		this.IBN = ibn;
		this.OBN = obn;
		this.TBT = tbt;
		this.FCN = fcn;
		this.FCR = fcr;
		this.address = address;
	}
	public boolean isVisited(){
		return visit;
	}
	public void setVisited(boolean flg){
		visit = flg;
	}
	public int getCid(){
		return cid;
	}
	public void setCid(int value){
		cid = value;
	}
	public String getVector(){
		return pair + " " + IPN + " " + OPN + " " + PPF + " " + TBF + " " + IBN + " " + OBN + " " + TBT + " " + FCN + " " + FCR;
	}
	
	public void setPair(String pair){
		this.pair = pair;
	}
	public void setIPN(String IPN){
		this.IPN = IPN;
	}
	public void setOPN(String OPN){
		this.OPN = OPN;
	}
	public void setPPF(String PPF){
		this.PPF = PPF;
	}
	public void setTBF(String TBF){
		this.TBF = TBF;
	}
	public void setIBN(String IBN){
		this.IBN = IBN;
	}
	public void setOBN(String OBN){
		this.OBN = OBN;
	}
	public void setTBT(String TBT){
		this.TBT = TBT;
	}
	public void setFCN(String FCN){
		this.FCN = FCN;
	}
	public void setFCR(String FCR){
		this.FCR = FCR;
	}
}
