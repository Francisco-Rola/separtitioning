package thesis;

import java.util.*;

public class Vertex {
	
	private HashSet<String> readSet = new HashSet<>();
	private HashSet<String> writeSet = new HashSet<>();
	
	public Vertex() {
		
	}
	
	public void addToReadSet(String formula) {
		readSet.add(formula);
	}
	
	public void addToWriteSet(String formula) {
		writeSet.add(formula);
	}
	
	
	public HashSet<String> getRhos() {
		HashSet<String> rhos = new HashSet<>();
		rhos.addAll(readSet);
		rhos.addAll(writeSet);
		return rhos;
	}
	
	
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
