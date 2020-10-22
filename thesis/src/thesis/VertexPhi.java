package thesis;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import javafx.util.Pair;

public class VertexPhi {
	
	private HashMap<String, Pair<Integer, Integer>> variables = null;
	
	private static int w = 2;
	
	public VertexPhi(VertexPhi vertexPhi) {
		this.variables = new HashMap<>();
		for (Map.Entry<String, Pair<Integer, Integer>> entry : vertexPhi.getPhi().entrySet()) {
			String newVar = new String(entry.getKey());
			Pair<Integer, Integer> newRanges = new Pair<Integer, Integer>(entry.getValue().getKey(), entry.getValue().getValue());
			this.variables.put(newVar, newRanges);
		}
	}
	
	public VertexPhi (HashSet<String> rhoVariables) {
		
		this.variables = new HashMap<>();
		
		for (String variable : rhoVariables) {
			if (variable.equals("districtid")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(1), new Integer(w*10));
				this.variables.put(variable, range);
			}
			else if (variable.equals("warehouseid")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(1), new Integer(w));
				this.variables.put(variable, range);
			}
			else if (variable.equals("customerid")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(1), new Integer(w*300));
				this.variables.put(variable, range);
			}
			else if (variable.startsWith("olsupplywid")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(1), new Integer(w));
				this.variables.put(variable, range);
			}
			else if (variable.startsWith("oliid")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(1), new Integer(w));
				this.variables.put(variable, range);
			}
			else if (variable.equals("irid")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(1), new Integer(w));
				this.variables.put(variable, range);
			}
			else {
				System.out.println("Missing case -> " + variable);
			}
		}
	}
	
	public HashMap<String, Pair<Integer, Integer>> getPhi() {
		return this.variables;
	}
	
	public String getPhiAsString() {
		String phi = "";
		for (Map.Entry<String, Pair<Integer,Integer>> entry: variables.entrySet()) {
			phi += entry.getValue().getKey() + " <= " + entry.getKey() + " <= " + entry.getValue().getValue() + " && ";
		}
		phi = phi.substring(0, phi.length() - 4);
		return phi;
	}
	
	public String getPhiAsGroup() {
		String phiQuery = "";
		// find each phi associated wit the rho
		for (Map.Entry<String, Pair<Integer, Integer>> phiEntry: this.variables.entrySet()) {
			phiQuery += "{" + phiEntry.getKey() + ", " + phiEntry.getValue().getKey() + ", " + phiEntry.getValue().getValue() + "} ,";
		}
		phiQuery = phiQuery.substring(0, phiQuery.length() - 2);
		return phiQuery;
	}
	
	public static int getTableRange(int tableNo) {
		switch (tableNo) {
		case 8:
			return w;
		case 9:
			return w * w;
		default:
			System.out.println("\n\nTable range unknown\n\n");
			return -1;
		}
	}
	
	public void printPhi() {
		for (Map.Entry<String, Pair<Integer, Integer>> entry: this.getPhi().entrySet()) {
			System.out.println("(" + entry.getKey() + ", " + entry.getValue().getKey() + ", " + entry.getValue().getValue() + ")");
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((variables == null) ? 0 : variables.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		VertexPhi other = (VertexPhi) obj;
		if (variables == null) {
			if (other.variables != null)
				return false;
		} else if (!variables.equals(other.variables))
			return false;
		return true;
	}
	
	

}
