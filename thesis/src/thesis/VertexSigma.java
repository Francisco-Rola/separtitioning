package thesis;

import java.util.*;

import com.wolfram.jlink.KernelLink;

// class that represents a formula mapping to the data items present in a vertex
public class VertexSigma {
	// ordered hash map that stores the mapping between each vertex rho and its corresponding vertex phi
	private LinkedHashMap<VertexRho, VertexPhi> rhos = new LinkedHashMap<>();
	
	// constructor for deep copy purpose
	public VertexSigma(VertexSigma sigma) {
		this.rhos = new LinkedHashMap<>();	
		for (Map.Entry<VertexRho, VertexPhi> rhoPhi : sigma.getRhos().entrySet()) {
			VertexRho rhoCopy = new VertexRho(rhoPhi.getKey());
			VertexPhi phiCopy = new VertexPhi(rhoPhi.getValue());
			this.rhos.put(rhoCopy, phiCopy);
		}
	}
	// default constructor for vertex sigma, built from a set of rho strings given by SE
	public VertexSigma(HashSet<String> rhos) {
		for (String rho: rhos) {
			// remove underscores from each rho variable
			String rhoUpdated = rho.replaceAll("_", "");
			// remove brackets from rho variables
			rhoUpdated = rhoUpdated.replaceAll("[\\[\\]]", "");
			// translate indirect reads into variables
			if (rhoUpdated.contains("GET") || rhoUpdated.startsWith("12->")) {
				// table 7 indirect read, new order
				if (rhoUpdated.startsWith("7->") && rhoUpdated.contains("->10")) {
					rhoUpdated = rhoUpdated.replaceAll("GET(.*?)->10\\)"
							, "iroliid"); 
					if (rhoUpdated.contains("1000000"))
						continue;
				}
	
				// table 7 indirect read, delivery
				else if (rhoUpdated.startsWith("7->"))
					rhoUpdated = rhoUpdated.replaceAll("\\(GET(.*?)\\s"
						, "(iroliid ");
				// table 5 indirect read, new order
				else if (rhoUpdated.startsWith("5->") && rhoUpdated.contains("->10"))
					rhoUpdated = rhoUpdated.replaceAll("GET(.*?)->10\\)"
						, "iroliid");
				// table 5 indirect reads, delivery
				else if (rhoUpdated.startsWith("5->") && rhoUpdated.contains("->1"))
					rhoUpdated = rhoUpdated.replaceAll("GET(.*?)->1\\)"
							, "iroliid");
				else if (rhoUpdated.startsWith("5->"))
					rhoUpdated = rhoUpdated.replaceAll("\\(GET(.*?)\\s"
							, "(iroliid "); 
				// table 6 indirect reads, new order
				else if (rhoUpdated.startsWith("6->") && rhoUpdated.contains("->10"))
					rhoUpdated = rhoUpdated.replaceAll("GET(.*?)->10\\)"
						, "iroliid");
				// table 6 indirect reads, delivery
				else if (rhoUpdated.startsWith("6->"))
					rhoUpdated = rhoUpdated.replaceAll("\\(GET(.*?)\\s"
							, "(iroliid ");				
				// delivery table 3 indirect reads
				else if (rhoUpdated.startsWith("3->"))
					rhoUpdated = rhoUpdated.replaceAll("GET(.*?)->3\\)"
							, "ircustomerid");
				else {
					continue;
				}
			}
			
			if (rhoRepeated(rhoUpdated)) {
				System.out.println("Rho repeated, increased value");
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
				if (entry.getKey().getRhoUpdate() != null) {
					phiQuery = "(" + phiQuery +  ") && (" + entry.getKey().getRhoUpdate() + ")";
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
	
	// automatically generated hash code for a vertex sigma
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((rhos == null) ? 0 : rhos.hashCode());
		return result;
	}
}
