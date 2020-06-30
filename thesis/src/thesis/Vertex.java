package thesis;

import java.util.*;

public class Vertex {
	
	private ArrayList<String> phis = new ArrayList<>();
	private ArrayList<String> readSet = new ArrayList<>();
	private ArrayList<String> writeSet = new ArrayList<>();
	
	public Vertex() {
		// TODO fix me, defaults phis for payment and new order
		phis.add("0 <= district_id < 9");
		phis.add("0 <= warehouse_id < 9");
		phis.add("0 <= client_id < 100");
	}
	
	public void addToReadSet(String formula) {
		readSet.add(formula);
	}
	
	public void addToWriteSet(String formula) {
		writeSet.add(formula);
	}
	
	public ArrayList<String> getPhis() {
		return this.phis;
	}
	
	public ArrayList<String> getRhos() {
		ArrayList<String> rhos = new ArrayList<>();
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
