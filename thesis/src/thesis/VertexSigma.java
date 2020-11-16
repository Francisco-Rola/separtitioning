package thesis;

import java.util.function.Predicate;
import java.util.*;

// class that represents a formula mapping to the data items present in a vertex
public class VertexSigma implements Predicate<Integer>{
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
			if (rhoUpdated.contains("GET")) {
				System.out.println(rhoUpdated);

				// table 7 indirect read, new order
				if (rhoUpdated.startsWith("7->") && rhoUpdated.contains("->10"))
					rhoUpdated = rhoUpdated.replaceAll("GET(.*?)->10\\)"
						, "iroliid");
				// table 7 indirect read, delivery
				else if (rhoUpdated.startsWith("7->"))
					rhoUpdated = rhoUpdated.replaceAll("\\(GET(.*?)\\s"
						, "(iroliid ");
				// table 5 indirect read, new order
				else if (rhoUpdated.startsWith("5->") && rhoUpdated.contains("->10"))
					rhoUpdated = rhoUpdated.replaceAll("GET(.*?)->10\\)"
						, "iroliid");
				// table 5 indirect reads, delivery
				else if (rhoUpdated.startsWith("5->") && rhoUpdated.contains("->3"))
					rhoUpdated = rhoUpdated.replaceAll("GET(.*?)->3\\)"
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
				System.out.println(rhoUpdated);
			}
			//build the rho from the updated string
			VertexRho vertexRho = new VertexRho(rhoUpdated);
			//build the phi from the rho's variables
			VertexPhi vertexPhi = new VertexPhi(vertexRho.getVariables());
			// store the mapping
			this.rhos.put(vertexRho, vertexPhi);
		}
	}
	
	// method used to extract a vertexp hi for a given vertex rho
	public VertexPhi getPhi(VertexRho rho) {
		return rhos.get(rho);
	}
	
	// getter for the map between rhos and phis
	public LinkedHashMap<VertexRho, VertexPhi> getRhos() {
		return this.rhos;
	}
	
	// debug purpose, prints all the sigma's rhos and phis
	public void printSigma() {
		for (Map.Entry<VertexRho, VertexPhi> entry: rhos.entrySet()) {
			entry.getKey().printRho();
			entry.getValue().printPhi();
		}
	}
	
	// sigma is a logical predicate, given a data item identifier evaluate to true if vertex contains item
	@Override
	public boolean test(Integer t) {		
		return false;
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
