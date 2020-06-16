package thesis;

import java.util.*;

public class Vertex {
	
	private ArrayList<String> readSet = new ArrayList<>();
	private ArrayList<String> writeSet = new ArrayList<>();
	
	public Vertex() {
		
	}
	
	public void addToReadSet(String formula) {
		readSet.add(formula);
	}
	
	public void addToWriteSet(String formula) {
		writeSet.add(formula);
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
