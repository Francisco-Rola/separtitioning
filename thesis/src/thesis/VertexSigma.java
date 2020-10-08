package thesis;

import java.util.function.Predicate;
import java.util.*;


public class VertexSigma implements Predicate<Integer>{
	
	private HashMap<VertexRho, VertexPhi> rhos = new HashMap<>();
	
	
	public VertexPhi getPhi(VertexRho rho) {
		return rhos.get(rho);
	}
	
	public void removeRho(VertexRho rho) {
		rhos.remove(rho);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((rhos == null) ? 0 : rhos.hashCode());
		return result;
	}

	public HashMap<VertexRho, VertexPhi> getRhos() {
		return this.rhos;
	}
	
	public VertexSigma(VertexSigma sigma) {
		this.rhos = new HashMap<>();
		
		for (Map.Entry<VertexRho, VertexPhi> rhoPhi : sigma.getRhos().entrySet()) {
			VertexRho rhoCopy = new VertexRho(rhoPhi.getKey());
			VertexPhi phiCopy = new VertexPhi(rhoPhi.getValue());
			this.rhos.put(rhoCopy, phiCopy);
		}
	}
	
	public VertexSigma(HashSet<String> rhos) {
		for (String rho: rhos) {
			String rhoUpdated = rho.replaceAll("_", "");
			rhoUpdated = rhoUpdated.replaceAll("[\\[\\]]", "");
			if (rhoUpdated.contains("GET")) {				
				rhoUpdated = rhoUpdated.replaceAll("GET.*->10\\)"
						, "irid");
			}
			VertexRho vertexRho = new VertexRho(rhoUpdated);
			VertexPhi vertexPhi = new VertexPhi(vertexRho.getVariables());
			this.rhos.put(vertexRho, vertexPhi);
		}
	}
	
	public void printSigma() {
		for (Map.Entry<VertexRho, VertexPhi> entry: rhos.entrySet()) {
			entry.getKey().printRho();
			entry.getValue().printPhi();
		}
	}
	
	@Override
	public boolean test(Integer t) {		
		return false;
	}
}
