package pcapflow;

import java.text.SimpleDateFormat;
import java.util.Date;

public class PerFlowStatus {
    public static int BIG_PACKET_THRESHOLD = 900;	
    public static int SMALL_PACKET_THRESHOLD = 30;
    public static boolean FIELD_NAME_OUTPUT = true;
	private String[] mFlowIPs = new String[2];
    private String[] mFlowPorts = new String[2];
    private String mType = "";
    
    private int mMTU = 0; // the number of reach MTU Packets
    private int mZeroPackets = 0;
    private int mIngoingPackets = 0; // the number of ingoing packets
    private int mOutgoingPackets = 0; // the number of outgoing packets 
    private int mPackets = 0; // the number of total packets
    private int mBigPackets = 0; // the number of big packets
    private int mSmallPackets = 0;
    private long mTotalLengthInBytes= 0; // TBT total number of bytes per flow
    private long mMinTimeInMilliseconds = -1;
    private long mMaxTimeInMilliseconds = -1;
    private long mTimeInterval = 0;

    private double mIORatio = 0.0f; // Ingoing / Outgoing Ratio
    private double mBigPacketRatio = 0.0f;
    private double mSmallPacketRatio = 0.0f;
    private double mZeroPacketRatio = 0.0f; // Zero Packet Ratio
    private double mAvgBytesPerPacket = 0.0f; //The average number of bytes per packet
    private double mAvgBytesPerSecond = 0.0f; 
    
    private static SimpleDateFormat tf = new SimpleDateFormat("HH:mm:ss");
    //-------------------------------------------------
    public PerFlowStatus(FlowFormat ff) {
        mFlowIPs[0] = ff.getSrcIP();
        mFlowIPs[1] = ff.getDstIP();
        mFlowPorts[0] = ff.getSrcPort();
        mFlowPorts[1] = ff.getDstPort();
        mType = ff.getType();
    }
    //-------------------------------------------------
    public void addFlowStatus(FlowFormat ff) {
        // mMTU UDP TCP
        if ( ff.getType().toLowerCase().equals("udp") ) {
            if ( ff.getLength() >= 1472 ) mMTU++;
        } else if ( ff.getType().toLowerCase().equals("tcp") ) {
            if ( ff.getLength() >= 1500 ) mMTU++;
        } 
        // zero packets
        if ( ff.getLength() <= 0 ) 
            mZeroPackets++;
        // Ingoing Packets and Outgoing Packets
        if ( ff.getSrcIP().equals( mFlowIPs[0] ) ) {
            mIngoingPackets ++;
        } else {
            mOutgoingPackets ++;
        }
        // total packets
        mPackets++;
        
        // the number of big packets
        if ( ff.getLength() > BIG_PACKET_THRESHOLD ) { 
            mBigPackets++;
        }
        if ( ff.getLength() > 0 && ff.getLength() < SMALL_PACKET_THRESHOLD ) {
            mSmallPackets++;
        }

        // total bytes per flow
        mTotalLengthInBytes += ff.getLength();
        try { 
        	Date time = tf.parse(ff.getTimestamp());
        	long timeInMilliseconds = time.getTime();
        	//--------------------------------------------------
        	// initial value
        	if ( mMinTimeInMilliseconds < 0 ) {
        		mMinTimeInMilliseconds = timeInMilliseconds;
        	}
        	if ( mMaxTimeInMilliseconds < 0 ) {
        		mMaxTimeInMilliseconds = timeInMilliseconds;
        	}
        	//--------------------------------------------------
        	if ( mMinTimeInMilliseconds > timeInMilliseconds ) {
        		mMinTimeInMilliseconds = timeInMilliseconds;
        	}
        	if ( mMaxTimeInMilliseconds < timeInMilliseconds) {
        		mMaxTimeInMilliseconds = timeInMilliseconds;
        	}
        	
        } catch ( Exception e ) {
        	e.printStackTrace();
        }
    }
    public void weighFeature() {
        double tmp1, tmp2;
        mTimeInterval = mMaxTimeInMilliseconds - mMinTimeInMilliseconds;
        tmp1 = (double)mIngoingPackets;
        tmp2 = (double)mOutgoingPackets;
        if ( tmp2 > 0.0f ) {
        	mIORatio = tmp1/tmp2;
        }
        
        tmp1 = (double)mBigPackets;
        tmp2 = (double)mPackets;
        if ( tmp2 > 0.0f ) {
        	mBigPacketRatio = tmp1/tmp2;
        }
        
        tmp1 = (double)mZeroPackets;
        tmp2 = (double)mPackets;
        if ( tmp2 > 0.0f ) {
        	mZeroPacketRatio = tmp1/tmp2;
        }
        
        tmp1 = (double)mSmallPackets;
        tmp2 = (double)mPackets;
        if ( tmp2 > 0.0f ) {
        	mSmallPacketRatio = tmp1/tmp2;
        }
        
        tmp1 = (double)mTotalLengthInBytes;
        tmp2 = (double)mPackets;
        if ( tmp2 > 0.0f ) {
        	mAvgBytesPerPacket = tmp1/tmp2;
        }
        
        tmp1 = (double)mTotalLengthInBytes;
        tmp2 = (double)mTimeInterval;
        tmp2 = tmp2/1000.0f;
        if ( tmp2 > 0.0f ) {
        	mAvgBytesPerSecond = tmp1/tmp2;
        }
    }
    //-------------------------------------------------
    @Override
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	//sb.append("perFlow");
    	//sb.append(" ");
    	sb.append(mFlowIPs[0]).append(":").append(mFlowPorts[0]);
    	sb.append(">");
        sb.append(mFlowIPs[1]).append(":").append(mFlowPorts[1]);
        sb.append(" ");
        if ( FIELD_NAME_OUTPUT ) { 
            sb.append("MTU:");// the MTU packet number
        }
        sb.append(mMTU);

        sb.append(" ");
        if ( FIELD_NAME_OUTPUT ) { 
            sb.append("NPN:");//the null packet number
        }
        sb.append(mZeroPackets);

        sb.append(" ");
        if ( FIELD_NAME_OUTPUT ) { 
            sb.append("IPN:");// the incoming packets number
        }
        sb.append(mIngoingPackets);

        sb.append(" ");
        if ( FIELD_NAME_OUTPUT ) { 
            sb.append("OPN:");// the outgoing packets number
        }
        sb.append(mOutgoingPackets);

        sb.append(" ");
        if ( FIELD_NAME_OUTPUT ) { 
            sb.append("PPF:"); //the number of packet per flow
        }
        sb.append(mPackets);

        sb.append(" ");
        if ( FIELD_NAME_OUTPUT ) { 
            sb.append("BPN:"); //the number of big packet
        }
        sb.append(mBigPackets);

        sb.append(" ");
        if ( FIELD_NAME_OUTPUT ) { 
            sb.append("SPN:"); //the number of small packet
        }
        sb.append(mSmallPackets);

        sb.append(" ");
        if ( FIELD_NAME_OUTPUT ) { 
            sb.append("TBT:"); //the total number bytes per flow 
        }
        sb.append(mTotalLengthInBytes);

        sb.append(" ");
        if ( FIELD_NAME_OUTPUT ) { 
            sb.append("APL:"); //average packet length per flow
        }
        sb.append(mAvgBytesPerPacket);

        sb.append(" ");
        if ( FIELD_NAME_OUTPUT ) { 
            sb.append("IOP:"); //the ratio of incoming packet and outgoing packers
        }
        sb.append(mIORatio);

        sb.append(" ");
        if ( FIELD_NAME_OUTPUT ) { 
            sb.append("NPR:"); //Null packets ratio in a flow
        }
        sb.append(mZeroPacketRatio);

        sb.append(" ");
        if ( FIELD_NAME_OUTPUT ) { 
            sb.append("BPR:"); //big packet ratio in flow
        }
        sb.append(mBigPacketRatio);

        sb.append(" ");
        if ( FIELD_NAME_OUTPUT ) { 
            sb.append("SPR:"); //small packet ratio in flow
        }
        sb.append(mSmallPacketRatio);

        sb.append(" ");
        if ( FIELD_NAME_OUTPUT ) { 
            sb.append("TBF:"); // flow's time interval
        }
        sb.append(mTimeInterval);

        sb.append(" ");
        if ( FIELD_NAME_OUTPUT ) { 
            sb.append("BPS:"); // flow's bytes per second
        }
        sb.append(mAvgBytesPerSecond);
        
        return sb.toString();
    }
    //-------------------------------------------------
    public boolean isFlowMatch( FlowFormat ff ) {
       return isIPMatch(ff) & isPortMatch(ff) & isTypeMatch(ff); 
    }
    public boolean isTypeMatch( FlowFormat ff ) {
        if ( mType.equals( ff.getType()) ) {
            return true;
        }
        return false;
    }
    public boolean isIPMatch( FlowFormat ff ) {
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
    public boolean isPortMatch( FlowFormat ff ) { 
        return isPortMatch( ff.getSrcPort(), ff.getDstPort() );
    }
    public boolean isPortMatch( String Port1, String Port2) {
        boolean b1 = false, b2 =false;
        for ( String Port : mFlowPorts ) {
            if ( Port1.equals(Port) ) {
                b1 = true;
            }
            if ( Port2.equals(Port) ) {
                b2 = true;
            }
        }
        return b1 & b2;
    }
    //-------------------------------------------------
    public int getmMTU() {
		return mMTU;
	}
	public int getmZeroPackets() {
		return mZeroPackets;
	}
	public int getmIngoingPackets() {
		return mIngoingPackets;
	}
	public int getmOutgoingPackets() {
		return mOutgoingPackets;
	}
	public int getmPackets() {
		return mPackets;
	}
	public int getmBigPackets() {
		return mBigPackets;
	}
	public int getmSmallPackets() {
		return mSmallPackets;
	}
	public long getmTotalLengthInBytes() {
		return mTotalLengthInBytes;
	}
	public long getmMinTimeInMilliseconds() {
		return mMinTimeInMilliseconds;
	}
	public long getmMaxTimeInMilliseconds() {
		return mMaxTimeInMilliseconds;
	}
	public long getmTimeInterval() {
		return mTimeInterval;
	}
	public double getmIORatio() {
		return mIORatio;
	}
	public double getmBigPacketRatio() {
		return mBigPacketRatio;
	}
	public double getmSmallPacketRatio() {
		return mSmallPacketRatio;
	}
	public double getmZeroPacketRatio() {
		return mZeroPacketRatio;
	}
	public double getmAvgBytesPerPacket() {
		return mAvgBytesPerPacket;
	}
	public double getmAvgBytesPerSecond() {
		return mAvgBytesPerSecond;
	}
}
