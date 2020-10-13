package thesis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import com.wolfram.jlink.KernelLink;



public class GraphVertex {
	
	private int vertexWeight = 0;
	
	private VertexSigma sigma;
	
	public GraphVertex(VertexSigma sigma) {
		this.sigma = sigma;	
	}
	
	public VertexSigma getSigma() {
		return sigma;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((sigma == null) ? 0 : sigma.hashCode());
		return result;
	}
	
	public void computeVertexWeight() {
		// get mathematica endpoint
		KernelLink link = MathematicaHandler.getInstance();
		// auxiliary structure to compute vertex weight
		HashMap<String, ArrayList<String>> tableAccesses = new HashMap<>();
		// auxiliary structure to compute rhos to remove
		HashSet<VertexRho> rhosToRemove = new HashSet<>();
		// iterate through rho and respective phis
		for (Map.Entry<VertexRho, VertexPhi> entry: this.getSigma().getRhos().entrySet()) {
			String table = entry.getKey().getRho().substring(0, entry.getKey().getRho().indexOf(">") - 1);
			String phiQuery = entry.getValue().getPhiAsGroup();
			String rhoQuery = entry.getKey().getRho().substring(entry.getKey().getRho().indexOf(">") + 1);
			// check if rho is constrained by logical subtraction
			if (entry.getKey().getRhoUpdate() != null) {
				System.out.println("Calculing weight for rho with modified clause");
				rhoQuery += " && !(" + entry.getKey().getRhoUpdate() + ")";
			}
			String query = "Flatten[Table[" + rhoQuery + ", " + phiQuery + "]]";
			String result = link.evaluateToOutputForm(query, 0);
			result = result.replaceAll("[, ]?False[, ]?", "");
			if (result.equals("{}")) {
				System.out.println("Empty rho, removing");
				rhosToRemove.add(entry.getKey());
				continue;
			}
			
			if (!tableAccesses.containsKey(table)) {
				ArrayList<String> accesses = new ArrayList<>();
				accesses.add(result);
				tableAccesses.put(table, accesses);
			}
			else {
				tableAccesses.get(table).add(result);
			}
		}
		
		// each table has its accesses computed, evaluate size of the access
		for (Map.Entry<String, ArrayList<String>> entry: tableAccesses.entrySet()) {
			String query = "";
			for (String access: entry.getValue()) {
				query += access + ", ";
			}
			query = query.substring(0, query.length() - 2);
			String mathQuery = "Length[DeleteDuplicates[Union[" + query + "]]]";
			String result = link.evaluateToOutputForm(mathQuery, 0);
			vertexWeight += Integer.parseInt(result);
		}
		/*
		// remove rhos that are now empty
		for (VertexRho rhoToRemove: rhosToRemove) {
			this.sigma.removeRho(rhoToRemove);
		}*/
	}
	
	public void printVertex() {
		System.out.println("--------------------------");
		System.out.println("Vertex weight: " + this.vertexWeight);
		this.sigma.printSigma();
	}
		
}