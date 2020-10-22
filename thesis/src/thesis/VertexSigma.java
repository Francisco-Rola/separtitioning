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
				rhoUpdated = rhoUpdated.replaceAll("GET.*->10\\)"
						, "irid");
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
