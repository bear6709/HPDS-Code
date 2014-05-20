package org.hpds.multiedge.feature;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AdjacencyList {
	private NodeContainer mNode;
	private List<NodeContainer> mList= new ArrayList<NodeContainer>();
	public AdjacencyList() {
		
	}
	public void setNode(NodeContainer node) {
		mNode = node;
	}
	public void addAdjacencyNode(NodeContainer node) {
		mList.add(node); 
	}
	public int getAdjacencyNodeSize() {
		return mList.size();
	}
	public void clear() {
		mNode = null;
		mList.clear();
	}
	@Override 
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(mNode.toCompactString());
		Iterator<NodeContainer> iterator = mList.iterator();
        while(iterator.hasNext()) {
        	NodeContainer nc = iterator.next();
        	sb.append(","+nc.toCompactString(mNode));
        }
		return sb.toString();
	}
}
