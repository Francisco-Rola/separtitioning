package thesis;

import java.util.*;

import com.wolfram.jlink.KernelLink;

// class that represents a formula mapping to the data items present in a vertex
public class VertexSigma {
	// ordered hash map that stores the mapping between each vertex rho and its corresponding vertex phi
	private LinkedHashMap<VertexRho, VertexPhi> rhos = new LinkedHashMap<>();
	
	// list of splits associated to a vertex
	private ArrayList<Split> splits = new ArrayList<>();
	
	// constructor for deep copy purpose
	public VertexSigma(VertexSigma sigma) {
		this.rhos = new LinkedHashMap<>();	
		for (Map.Entry<VertexRho, VertexPhi> rhoPhi : sigma.getRhos().entrySet()) {
			VertexRho rhoCopy = new VertexRho(rhoPhi.getKey());
			VertexPhi phiCopy = new VertexPhi(rhoPhi.getValue());
			this.rhos.put(rhoCopy, phiCopy);
		}
		for (Split split: sigma.getSplits()) {
			Split splitCopy = new Split(split.getSplitParam(), split.getLowerBound(), split.getUpperBound());
			this.splits.add(splitCopy);
		}
	}
	// default constructor for vertex sigma, built from a set of rho strings given by SE
	public VertexSigma(HashSet<String> rhos) {
		for (String rho: rhos) {
			// skip indexes
			if (Integer.parseInt(rho.substring(0, rho.indexOf(">") - 1)) > 9) continue;
			// remove underscores from each rho variable
			String rhoUpdated = rho.replaceAll("_", "");
			// remove brackets from rho variables
			rhoUpdated = rhoUpdated.replaceAll("[\\[\\]]", "");
			// translate indirect reads into variables
			if (rhoUpdated.contains("GET") || rhoUpdated.startsWith("12->")) {
				
				// TPC C 
				if (rhoUpdated.contains("Tpcc")) {
				
					// table 7 indirect read, new order
					if (rhoUpdated.startsWith("7->") && rhoUpdated.contains("Tpcc:79")) {
						rhoUpdated = rhoUpdated.replaceAll("GET-0@Tpcc:79"
								, "irorderid"); 
					}
					// table 7 indirect read, stock level
					else if (rhoUpdated.startsWith("7->") && rhoUpdated.contains("->10")) {
						if (rhoUpdated.contains(" - "))
								rhoUpdated = rhoUpdated.replaceAll("\\(GET(.*?)-(\\s\\d+?)\\)"
								, "irorderid");
						else
							rhoUpdated = rhoUpdated.replaceAll("\\(GET(.*?)->10\\)"
								, "(irorderid");
					}
					// table 7 indirect read, order status
					else if (rhoUpdated.startsWith("7->") && rhoUpdated.contains("->0")) 
						rhoUpdated = rhoUpdated.replaceAll("\\(GET(.*?)->0\\)\\s\\*\\s1000000"
								, "(irorderid * 1000000");
						
					// table 7 indirect read, delivery
					else if (rhoUpdated.startsWith("7->"))
						rhoUpdated = rhoUpdated.replaceAll("\\(GET(.*?)->1\\)"
							, "(irorderid");
					// table 5 indirect read, new order
					else if (rhoUpdated.startsWith("5->") && rhoUpdated.contains("->10"))
						rhoUpdated = rhoUpdated.replaceAll("GET(.*?)->10\\)"
							, "irorderid");
					// table 5 indirect reads, delivery
					else if (rhoUpdated.startsWith("5->") && rhoUpdated.contains("->1"))
						rhoUpdated = rhoUpdated.replaceAll("GET(.*?)->1\\)"
								, "irorderid");
					// table 5 indirect reads, delivery v2
					else if (rhoUpdated.startsWith("5->"))
						rhoUpdated = rhoUpdated.replaceAll("\\(GET(.*?)\\s"
								, "(irorderid"); 
					// table 6 indirect reads, new order
					else if (rhoUpdated.startsWith("6->") && rhoUpdated.contains("->10"))
						rhoUpdated = rhoUpdated.replaceAll("GET(.*?)->10\\)"
							, "irorderid");
					// table 6 indirect read, order status
					else if (rhoUpdated.startsWith("6->") && rhoUpdated.contains("->0")) 
						rhoUpdated = rhoUpdated.replaceAll("\\(GET(.*?)->0\\)"
								, "(irorderid");
					// table 6 indirect reads, delivery
					else if (rhoUpdated.startsWith("6->"))
						rhoUpdated = rhoUpdated.replaceAll("\\(GET(.*?)->1\\)"
								, "(irorderid");				
					// delivery table 3 indirect reads
					else if (rhoUpdated.startsWith("3->"))
						rhoUpdated = rhoUpdated.replaceAll("GET(.*?)->3\\)"
								, "ircustomerid");
					// delivery table 9 indirect reads
					else if (rhoUpdated.startsWith("9->"))
						rhoUpdated = rhoUpdated.replaceAll("GET(.*?)->4\\)"
								, "iroliid");
				}
				// regiteritem rubis
				else if (rhoUpdated.startsWith("2->")) {
					rhoUpdated = rhoUpdated.replaceAll("GET(.*?)categoryId->2\\)", 
							"iritemid");
				}
				// registeruser rubis
				else if (rhoUpdated.startsWith("1->")) {
					rhoUpdated = rhoUpdated.replaceAll("GET(.*?)regionId->2\\)", 
							"iruserid");
				}
				else if (rhoUpdated.startsWith("5->")) {
					rhoUpdated = rhoUpdated.replaceAll("GET(.*?)Rubis\\:157", 
							"irbidid");
				}
				else if (rhoUpdated.startsWith("6->")) {
					rhoUpdated = rhoUpdated.replaceAll("GET(.*?)Rubis\\:106", 
							"irbuyid");
				}
				else if (rhoUpdated.startsWith("7->")) {
					rhoUpdated = rhoUpdated.replaceAll("GET(.*?)Rubis\\:199", 
							"ircommentid");
				}
				else {
					continue;
				}
			}
			
			if (rhoRepeated(rhoUpdated)) {
				continue;
			}
				
			//build the rho from the updated string
			VertexRho vertexRho = new VertexRho(rhoUpdated);
			//build the phi from the rho's variables
			VertexPhi vertexPhi = new VertexPhi(vertexRho.getVariables());
			
			if (vertexPhi.getPhi().size() == 0) {
				System.out.println(rhoUpdated);
			}
			// store the mapping
			this.rhos.put(vertexRho, vertexPhi);
		}
	}
	
	// method that adds a split to this vertex
	public void addSplit(Split split) {
		splits.add(split);
	}
	
	// method that returns splits for vertex
	public ArrayList<Split> getSplits() {
		return this.splits;
	}
	
	// method that merges similar rhos into same one OPTIMIZATION
	private boolean rhoRepeated(String rho) {
		for (Map.Entry<VertexRho, VertexPhi> entry: rhos.entrySet()) {
			if (entry.getKey().getRho().equals(rho)) {
				entry.getKey().setValue();
				return true;
			}
		}
		return false;
	}
	
	// method used to extract a vertex phi for a given vertex rho
	public VertexPhi getPhi(VertexRho rho) {
		return rhos.get(rho);
	}
	
	// getter for the map between rhos and phis
	public LinkedHashMap<VertexRho, VertexPhi> getRhos() {
		return this.rhos;
	}
	
	// check whether VertexSigma contains a given key
	public boolean containsKey(String key, String table) {
		
		KernelLink link = MathematicaHandler.getInstance();
		
		for (Map.Entry<VertexRho, VertexPhi> entry: rhos.entrySet()) {
			if (entry.getKey().getTable().equals(table)) {
				// tables match, need to ask Mathematica if key is in table
				String phiQuery = entry.getValue().getPhiAsString();
				String rhoQuery = entry.getKey().getRho().substring(entry.getKey().getRho().indexOf(">") + 1);
				// check if rho is constrained by logical subtraction
				if (entry.getKey().tableSplitExists()) {
					phiQuery = "(" + phiQuery +  ") && (" + entry.getKey().getTableSplitMath(rhoQuery) + ")";
				}
				// get variable set for Mathematica
				HashSet<String> vars = entry.getKey().getVariables();
				String variables = "{";
				for (String variable: vars) {
					variables += variable + ", ";
				}
				// remove extra characters and finalize string
				variables = variables.substring(0, variables.length() - 2) + "}";
				
				String query = "FindInstance[" + rhoQuery + " == " + key + " && " + phiQuery + 
						", " + variables + ", Integers]";
				
				// System.out.println(query);
				String result = link.evaluateToOutputForm(query, 0);
				// System.out.println(result);
				if (result.equals("{}")) {
					continue;
				}
				else {
					return true;
				}
			}
			else {
				continue;
			}
		}
		return false;
	}
	
	// debug purpose, prints all the sigma's rhos and phis
	public void printSigma() {
		for (Map.Entry<VertexRho, VertexPhi> entry: rhos.entrySet()) {
			if (!entry.getKey().isRemote()) {
				entry.getKey().printRho();
				entry.getValue().printPhi();
			}
		}
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((rhos == null) ? 0 : rhos.hashCode());
		result = prime * result + ((splits == null) ? 0 : splits.hashCode());
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
		VertexSigma other = (VertexSigma) obj;
		if (rhos == null) {
			if (other.rhos != null)
				return false;
		} else if (!rhos.equals(other.rhos))
			return false;
		if (splits == null) {
			if (other.splits != null)
				return false;
		} else if (!splits.equals(other.splits))
			return false;
		return true;
	}
	
	
}
