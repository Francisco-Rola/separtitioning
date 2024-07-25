package thesis;

import java.util.HashSet;

// class that represents SE tree node to aid parsing
public class SETreeNode {
	// left subtree pc
	String pcLeft = null;
	// right subtree pc
	String pcRight = null;
	
	public HashSet<String> getReadSet() {
		return readSet;
	}

	public HashSet<String> getWriteSet() {
		return writeSet;
	}

	public String getPcLeft() {
		return pcLeft;
	}

	public String getPcRight() {
		return pcRight;
	}

	// read set of the node
	HashSet<String> readSet = new HashSet<>();
	// write set of the node
	HashSet<String> writeSet = new HashSet<>();
	
	// default constructor for SE Node
	public SETreeNode(String pcLeft, String pcRight) {
		this.pcLeft = pcLeft;
		this.pcRight = pcRight;
	}
	
	// method to add readSet to a SE Node
	public void setReadSet(HashSet<String> readSet) {
		this.readSet.addAll(readSet);
	}
	
	// method to add writeSet to a SE Node
	public void setWriteSet(HashSet<String> writeSet) {
		this.writeSet.addAll(writeSet);
	}
	
	// method to check if SE node is a leaf node
	public boolean isLeaf() {
		if (pcLeft == null & pcRight == null) {
			return true;
		}
		return false;
	}

}
