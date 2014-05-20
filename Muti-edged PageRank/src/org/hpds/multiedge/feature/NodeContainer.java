package org.hpds.multiedge.feature;

public class NodeContainer {
	private String mNodeID;
	private float mAvgInterval;
	private float mIORatio;
	private float mPacketPerSec;
	public NodeContainer(String id, float avgInterval, float ioRatio, float packetPerSec) {
		mNodeID = id;
		mAvgInterval = avgInterval;
		mIORatio = ioRatio;
		mPacketPerSec = packetPerSec;
	}
	public NodeContainer(String line) {
		parse(line);
	}
	//35_140.116.164.86.12741_80.12.154.13.58321
	private void parse(String line) {
		String[] split = line.split(" |,|\t");
		//for ( String s : split ) {
			//System.out.println(s);
		//}
		if ( split.length == 4 ) {
			mNodeID = split[0];
			mAvgInterval = Float.valueOf(split[1]);
			mIORatio = Float.valueOf(split[2]);
			mPacketPerSec = Float.valueOf(split[3]);
		}
	}
	public String getNodeID() {
		return mNodeID;
	}
	public float getAvgInterval() {
		return mAvgInterval;
	}
	public float getIORatio() {
		return mIORatio;
	}
	public float getPacketPerSec() {
		return mPacketPerSec;
	}
	@Override
	public String toString() {
		String out ="ID: "+ mNodeID + "\tAvgInterval: " + mAvgInterval + "\tIORatio:" + mIORatio + "\tPacketPerSec: " + mPacketPerSec;
		return out;
	}
	public String toCompactString(NodeContainer p) {
		String out = mNodeID+"@["+Math.abs(p.mAvgInterval-mAvgInterval)+","+Math.abs(p.mIORatio-mIORatio)+","+Math.abs(p.mPacketPerSec-mPacketPerSec)+"]";
		return out;
	}
	public String toCompactString() {
		String out = mNodeID+"@["+mAvgInterval+","+mIORatio+","+mPacketPerSec+"]";
		return out;
	}
	
}
