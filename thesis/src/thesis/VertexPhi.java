package thesis;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import javafx.util.Pair;

// class that represents the constrains on each variable in SE formulas
public class VertexPhi {
	// set of variables and its ranges
	private HashMap<String, Pair<Integer, Integer>> variables = null;
	// scale factor for TPC -C
	private static int w = 2;
	
	// constructor for deep copy purpose
	public VertexPhi(VertexPhi vertexPhi) {
		this.variables = new HashMap<>();
		for (Map.Entry<String, Pair<Integer, Integer>> entry : vertexPhi.getPhi().entrySet()) {
			String newVar = new String(entry.getKey());
			Pair<Integer, Integer> newRanges = new Pair<Integer, Integer>(entry.getValue().getKey(), entry.getValue().getValue());
			this.variables.put(newVar, newRanges);
		}
	}
	
	// default constructor for vertex phi
	public VertexPhi (HashSet<String> rhoVariables) {
		// initialize variables map
		this.variables = new HashMap<>();
		// from all the variables in the corresponding rho, initialize its range
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
				// debug scenario, missing variable case
				System.out.println("Missing case -> " + variable);
			}
		}
	}
	
	// method using to extract set of variables in a phi and its ranges
	public HashMap<String, Pair<Integer, Integer>> getPhi() {
		return this.variables;
	}
	
	// method that returns vertex phi as a set of conjunctions comprising all its variables and ranges
	public String getPhiAsString() {
		String phi = "";
		for (Map.Entry<String, Pair<Integer,Integer>> entry: variables.entrySet()) {
			phi += entry.getValue().getKey() + " <= " + entry.getKey() + " <= " + entry.getValue().getValue() + " && ";
		}
		phi = phi.substring(0, phi.length() - 4);
		return phi;
	}
	
	// method that returns vertex phi as a sring that groups variables and its ranges in a string list
	public String getPhiAsGroup() {
		String phiQuery = "";
		// find each phi associated wit the rho
		for (Map.Entry<String, Pair<Integer, Integer>> phiEntry: this.variables.entrySet()) {
			phiQuery += "{" + phiEntry.getKey() + ", " + phiEntry.getValue().getKey() + ", " + phiEntry.getValue().getValue() + "} ,";
		}
		phiQuery = phiQuery.substring(0, phiQuery.length() - 2);
		return phiQuery;
	}
	
	// schema helper, obtain table max range for vertex splitting purposes
	public static int getTableRange(int tableNo) {
		switch (tableNo) {
		case 8:
			return w;
		case 9:
			return w * w;
		default:
			// debug scenario, if needed add more tables
			System.out.println("\n\nTable range unknown\n\n");
			return -1;
		}
	}
	
	// debug method, prints phi, all its variables and ranges
	public void printPhi() {
		for (Map.Entry<String, Pair<Integer, Integer>> entry: this.getPhi().entrySet()) {
			System.out.println("(" + entry.getKey() + ", " + entry.getValue().getKey() + ", " + entry.getValue().getValue() + ")");
		}
	}

	// automatically generated hash code for vertex phi
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((variables == null) ? 0 : variables.hashCode());
		return result;
	}

	// automatically generated equals for vertex phi
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
