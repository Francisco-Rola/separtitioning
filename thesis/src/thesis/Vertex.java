package thesis;

import java.util.*;

// class that represents a symbolic vertex before transformation into a graph vertex
public class Vertex {
	// read set for a given vertex, rho formulas
	private HashSet<String> readSet = new HashSet<>();
	// write set for a given vertex,rho formulas
	private HashSet<String> writeSet = new HashSet<>();
	// transaction profile id
	private int txProfile;
	
	// default constructor
	public Vertex(int txProfile) {
		this.txProfile = txProfile;
	}
	
	// getter for tx profile
	public int getTxProfile() {
		return this.txProfile;
	}
	
	// method to add a formula to a vertex's read set
	public void addToReadSet(String formula) {
		readSet.add(formula);
	}
	
	// method to add a formula to a vertex's write set
	public void addToWriteSet(String formula) {
		writeSet.add(formula);
	}
	
	// method that returns all the rhos in a vertex, no duplicates
	public HashSet<String> getRhos() {
		// deal with duplicate formulas by storing them in a set
		HashSet<String> rhos = new HashSet<>();
		rhos.addAll(readSet);
		rhos.addAll(writeSet);
		return rhos;
	}
	
	// debug purposes, prints read and write set for a vertex
	public void printVertex() {
		System.out.println("\tRead set");
		for(String formula : readSet) {
			System.out.println("\t\t" + formula);
		}
		System.out.println("\tWrite set");
		for(String formula : writeSet) {
			System.out.println("\t\t" + formula);
		}
	}
	
}
