package pcapflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class FlowCollector {
    private String[] mFlowIPs = new String[2];
    private float mAPLPF = 0; // Average packet length per flow
    private long mBPS = 0; // bytes per second
    private long mFPH = 0; // flows per hour 
    private List<PerFlowStatus> list = new ArrayList<PerFlowStatus>();
    //-------------------------------------------------
    public FlowCollector(FlowFormat ff) {
        mFlowIPs[0] = ff.getSrcIP();
        mFlowIPs[1] = ff.getDstIP();
    }
    public void addFlow(FlowFormat ff) {
    	PerFlowStatus pfs = null;
    	if ( list.size() <= 0 ) {
    		list.add(new PerFlowStatus(ff));
    	}
    	for ( PerFlowStatus f : list ) {
    		if ( f.isFlowMatch(ff) ) {
    			pfs = f ;
    			break;
    		}
    	}
    	if ( pfs == null ) {
    		pfs = new PerFlowStatus(ff);
    		list.add(pfs);
    	}
    	pfs.addFlowStatus(ff);
    }
    public void outputResult(Reducer<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
    	Text outKey = new Text("perFlow");
    	Text outValue = new Text();
    	for ( PerFlowStatus f : list ) {
    		outValue.set(f.toString());
    		context.write(outKey, outValue);
    	}
    	
    }
    //-------------------------------------------------
    public void weighAllFeatures() {
    	for ( PerFlowStatus f : list ) {
    		f.weighFeature();
    	}
    }
    //-------------------------------------------------
    public boolean isIPMatch(FlowFormat ff ) {
    	return isIPMatch( ff.getSrcIP(), ff.getDstIP() );
    }
    public boolean isIPMatch( String IP1, String IP2) {
        boolean b1 = false, b2 =false;
        for ( String IP : mFlowIPs ) {
            if ( IP1.equals(IP) ) {
                b1 = true;
            }
            if ( IP2.equals(IP) ) {
                b2 = true;
            }
        }
        return b1 & b2;
    }
    //-------------------------------------------------

}
