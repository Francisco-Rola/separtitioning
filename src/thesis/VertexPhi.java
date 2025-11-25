package thesis;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

// class that represents the constrains on each variable in SE formulas
public class VertexPhi {
	// set of variables and its ranges
	private HashMap<String, Pair<Integer, Integer>> variables = null;
	// scale factor for TPC -C
	private static int w = -1;
	private static int item = 99999;
	private static int customer = 2999;
	private static int district = 9;
	
	// scale factor for RUBIS
	private static int rubisItems = 99999;
	private static int rubisCategory = 9;
	private static int rubisUsers = 99999;
	private static int rubisRegion = 59;
	private static int rubisBid = 9;
	private static int rubisBuy = 9;
	private static int rubisComment = 9;
	
	// getters for scaling factors TPCC
	public static int getScalingFactorW() {
		return w + 1;
	}
	
	public static int getScalingFactorD() {
		return district + 1;
	}
	
	public static int getScalingFactorC() {
		return customer + 1;
	}
	
	public static int getScalingFactorI() {
		return item + 1;
	}
	
	// getters for scaling factos RUBIS
	public static int getRubisItems() {
		return rubisItems + 1;
	}
	
	public static int getRubisCategory() {
		return rubisCategory + 1;
	}
	
	public static int getRubisUsers() {
		return rubisUsers + 1;
	}
	
	public static int getRubisRegion() {
		return rubisRegion + 1;
	}
	
	public static int getRubisBid() {
		return rubisBid + 1;
	}
	
	public static int getRubisBuy() {
		return rubisBuy + 1;
	}
	
	public static int getRubisComment() {
		return rubisComment + 1;
	}
	
	
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
		
		this.w = GraphBuilder.noW;
		
		if (rhoVariables.size() == 0) System.out.println("bug");
		
		// initialize variables map
		this.variables = new HashMap<>();
		// from all the variables in the corresponding rho, initialize its range
		for (String variable : rhoVariables) {
			if (variable.equals("districtid")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(0), new Integer(district));
				this.variables.put(variable, range);
			}
			else if (variable.equals("warehouseid")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(0), new Integer(w));
				this.variables.put(variable, range);
			}
			else if (variable.equals("customerid")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(0), new Integer(customer));
				this.variables.put(variable, range);
			}
			else if (variable.startsWith("olsupplywid")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(0), new Integer(w));
				this.variables.put(variable, range);
			}
			else if (variable.startsWith("oliid")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(0), new Integer(item));
				this.variables.put(variable, range);
			}
			else if (variable.equals("irorderid")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(0), new Integer(customer));
				this.variables.put(variable, range);
			}
			else if (variable.equals("iroliid")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(0), new Integer(item));
				this.variables.put(variable, range);
			}
			else if (variable.equals("ircustomerid")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(0), new Integer(customer));
				this.variables.put(variable, range);
			}
			else if (variable.equals("iritemid")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(0), new Integer(rubisItems));
				this.variables.put(variable, range);
			}
			else if (variable.equals("categoryId")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(0), new Integer(rubisCategory));
				this.variables.put(variable, range);
			}
			else if (variable.equals("iruserid")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(0), new Integer(rubisUsers));
				this.variables.put(variable, range);
			}
			else if (variable.equals("regionId")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(0), new Integer(rubisRegion));
				this.variables.put(variable, range);
			}
			else if (variable.equals("irbidid")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(0), new Integer(rubisBid));
				this.variables.put(variable, range);
			}
			else if (variable.equals("irbuyid")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(0), new Integer(rubisBuy));
				this.variables.put(variable, range);
			}
			else if (variable.equals("itemId")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(0), new Integer((rubisCategory * 100000) + rubisItems));
				this.variables.put(variable, range);
			}
			else if (variable.equals("ircommentid")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(0), new Integer(rubisComment));
				this.variables.put(variable, range);
			}
			else if (variable.equals("fromId")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(0), new Integer((rubisRegion * 100000) + rubisUsers));
				this.variables.put(variable, range);
			}
			else if (variable.equals("toId")) {
				Pair<Integer, Integer> range = new Pair<Integer,Integer>(new Integer(0), new Integer((rubisRegion * 100000) + rubisUsers));
				this.variables.put(variable, range);
			}
			else {
				// debug scenario, missing variable case
				System.out.println("Missing case -> " + variable);
			}
		}
		if (this.variables.size() == 0) System.out.println("bug2");

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
		// compute max id of each table
		switch (tableNo) {
		case 1:
			if (w == -1 )
				return (rubisRegion * 100000) + rubisUsers;
			return w;
		case 5:
			return w * 100 + customer * 10000 + district;
		case 6:
			return w * 100 + customer * 10000 + district;
		case 7:
			return w * 100 + customer * 10000 + district + 15000;
		case 8:
			return item;
		case 9:
			return item + (w * 100000);
		default:
			// debug scenario, if needed add more tables
			System.out.println("\n\nTable range unknown\n\n" + tableNo);
			return -1;
		}
	}
	
	// schema helper, check if a table is read 
	public static boolean checkTableReplicated(int tableNo) {		
		switch (tableNo) {
		case 1:
			return false;
		case 8:
			// table 8 is ready only
			return true;
		default:
			// every other table has write accesses
			return false;
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
