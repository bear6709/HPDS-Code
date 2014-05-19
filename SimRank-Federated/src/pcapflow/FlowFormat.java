package pcapflow;

//08:14:43.237338 IP 211.72.192.30.8444 > 140.116.164.86.62537: UDP, length 436
//08:14:43.343047 IP 69.171.246.16.80 > 140.116.164.97.55546: Flags [P.], seq 2226618919:2226619686, ack 1131943935, win 64240, length 767
public class FlowFormat {
   
	private String mRaw = "none";
    private String mTimestamp = "none";
    private String mTimeFlag = "none";
    private String mProto = "none"; // IP or ARP
    private String mSrcIP = "none"; // src IP
    private String mSrcPort = "none"; // src Port
    private String mDstIP = "none"; // dst IP
    private String mDstPort = "none"; // dst Port
    private String mType = "none"; // UDP or TCP
    private int iLength =-1 ;
    //-----------------------------------
    // used in map phrase
    public void parsePCAP(String[] tokens) {
        int dotPos = 0;
        int commasPos = 0;
        setTimestamp( tokens[0] );
        setProto( tokens[1] );
        
        //parse Source IP and its port
        dotPos = tokens[2].lastIndexOf(".");
        if ( dotPos >= 0 ) {
            setSrcIP( tokens[2].substring(0, dotPos) );
            setSrcPort( tokens[2].substring( dotPos+1 ) );
        }
        //parse Destination IP and its port
        dotPos = tokens[4].lastIndexOf(".");
        commasPos = tokens[4].lastIndexOf(":");
        if ( dotPos >= 0 ) {
            setDstIP( tokens[4].substring(0, dotPos) );
            if ( commasPos >= 0 ) {
                setDstPort( tokens[4].substring(dotPos+1, commasPos) );
            } else {
                setDstPort( tokens[4].substring(dotPos+1) );
            }
        }
        setType("TCP");
        // parse protocol and packet length
        for ( int i = 5 ; i < tokens.length; i++ ) {
            if ( tokens[i].toLowerCase().startsWith("udp") ) {
                commasPos = tokens[5].lastIndexOf(",");
                if ( commasPos >= 0 ) {
                    setType( tokens[5].substring(0, commasPos) );
                }
            }
            if ( tokens[i].toLowerCase().startsWith("length") ) {
                try {
                    setLength(Integer.parseInt(tokens[i+1]));
                } catch ( Exception e ) {
                    e.printStackTrace();
                }
            }
        }
    }
    //-----------------------------------
    // used in reduce phrase
    public void parseInternal(String[] tokens) {
    		setTimestamp( "none" );
    		setProto( "none" );
    		setSrcIP( "none" );
    		setSrcPort( "none" );
    		setDstIP( "none" );
    		setDstPort( "none" );
    		setType( "none" );
    		setLength ( -1 );
    }
    //-----------------------------------
    public long getKeyHash() {
        long hash = 7777;
        hash += getSrcIP().hashCode();
        //hash += getSrcPort().hashCode();
        hash += getDstIP().hashCode();
        //hash += getDstPort().hashCode();
        return hash;
    }
    // internal format
    // separated by space
    @Override
    public String toString() {
    	StringBuilder sb = new StringBuilder();
        sb.append( getTimestamp() );
        sb.append(" ");
        sb.append( getProto() );
        sb.append(" ");
        sb.append( getSrcIP() );
        sb.append(" ");
        sb.append( getSrcPort() );
        sb.append(" ");
        sb.append( getDstIP() );
        sb.append(" ");
        sb.append( getDstPort() );
        sb.append(" ");
        sb.append( getType() );
        sb.append(" ");
        sb.append( getLength() );
        return sb.toString();
    }
    //-----------------------------------
    public FlowFormat() {
    	
    }
	public FlowFormat(String value, boolean PCAPformat){
		parse(value,PCAPformat);
	}
	public void parse(String value, boolean PCAPformat) {
		String[] tokens = value.split(" ");
		setRaw(mRaw);
		if ( PCAPformat ) {
			if ( tokens != null && tokens.length > 5 ) {
				if ( tokens[1].equals("IP") ) {
					try { 
						parsePCAP(tokens);
					} catch ( Exception e ) {
						System.out.println("parse error in " + value);
						System.exit(1);
					}
				}     
			}    
		} else {
			parseInternal( tokens );
		}
	}
	//------------------------------------
	//getter and setter
	public String getRaw() {
		return mRaw;
	}
	public void setRaw(String mRaw) {
		this.mRaw = mRaw;
	}
    public String getTimestamp() {
        return this.mTimestamp;
    }
    public void setTimestamp(String ts) {
        this.mTimestamp = ts;
    }
	public String getTimeFlag() {
		return mTimeFlag;
	}
	public void setTimeFlag(String mTimeFlag) {
		this.mTimeFlag = mTimeFlag;
	}
	public String getProto() {
		return mProto;
	}
	public void setProto(String mProto) {
		this.mProto = mProto;
	}
	public String getSrcIP() {
		return mSrcIP;
	}
	public void setSrcIP(String mSrcIP) {
		this.mSrcIP = mSrcIP;
	}
	public String getSrcPort() {
		return mSrcPort;
	}
	public void setSrcPort(String mSrcPort) {
		this.mSrcPort = mSrcPort;
	}
	public String getDstIP() {
		return mDstIP;
	}
	public void setDstIP(String mDstIP) {
		this.mDstIP = mDstIP;
	}
	public String getDstPort() {
		return mDstPort;
	}
	public void setDstPort(String mDstPort) {
		this.mDstPort = mDstPort;
	}
	public String getType() {
		return mType;
	}
	public void setType(String mType) {
		this.mType = mType;
	}
	public int getLength() {
		return iLength;
	}
	public void setLength(int length) {
		this.iLength = length;
	}
}
