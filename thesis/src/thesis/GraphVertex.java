package thesis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.wolfram.jlink.KernelLink;

// class that represents a graph vertex
public class GraphVertex {
	// vertex ID counter
	private static int vertexIDcounter = 1;
	// vertex ID
	private int vertexID = 0;
	// number of data items stored in the vertex
	private int vertexWeight = 0;
	// transaction profile that generated this vertex originally, inheried from parent in case of splitting
	private int txProfile;
	// formulas that represent the data items in this vertex	
	private VertexSigma sigma;
	
	// default constructor for a graph vertex
	public GraphVertex(VertexSigma sigma, int txProfile) {
		this.sigma = sigma;	
		this.txProfile = txProfile;
	}
	
	// constructor with ID
	public GraphVertex(VertexSigma sigma, int txProfile, boolean ID) {
		this.sigma = sigma;	
		this.txProfile = txProfile;
		this.vertexID = vertexIDcounter;
		vertexIDcounter++;
	}
	
	// getter for transaction profile
	public int getTxProfile() {
		return this.txProfile;
	}
	
	// getter for transaction profile
	public int getVertexID() {
		return this.vertexID;
	}
	
	// getter for vertex weeight
	public int getVertexWeight() {
		return this.vertexWeight;
	}
	
	// getter for vertex sigma
	public VertexSigma getSigma() {
		return sigma;
	}
	
	// method that computes vertex weight in SMT implementation
	public void computeVertexWeightSMT() {
		// temp variable for vertex weight
		int vertexWeightTemp = 0;
		// iterate over all the rhos in the vertex
		for (Map.Entry<VertexRho, VertexPhi> entry: this.getSigma().getRhos().entrySet()) {
			vertexWeightTemp += (entry.getKey().getNoItems() * entry.getKey().getValue());
		}
		this.vertexWeight = vertexWeightTemp;		
	}
	
	// method that computes vertex weight, i.e. how many data items it stores
	public void computeVertexWeight() {
		// get mathematica endpoint
		KernelLink link = MathematicaHandler.getInstance(); 
		// auxiliary structure to compute vertex weight
		HashMap<String, ArrayList<String>> tableAccesses = new HashMap<>();
		// iterate through rho and respective phis
		for (Map.Entry<VertexRho, VertexPhi> entry: this.getSigma().getRhos().entrySet()) {
			// skip low prob rhos they are always remote
			if (entry.getKey().getProb() < 0.5) 
				continue;
			String table = entry.getKey().getRho().substring(0, entry.getKey().getRho().indexOf(">") - 1);
			
			if (Integer.valueOf(table) > 9) continue;
			
			String phiQuery = entry.getValue().getPhiAsGroup();
			String rhoQuery = entry.getKey().getRho().substring(entry.getKey().getRho().indexOf(">") + 1);
			// check if rho is constrained by logical subtraction
			if (entry.getKey().getRhoUpdate() != null) {
				rhoQuery = "(" + rhoQuery +  ") && (" + entry.getKey().getRhoUpdate() + ")";
			}
			// check items accessed by rho given phi
			String query = "Flatten[Table[" + rhoQuery + ", " + phiQuery + "]]";
			System.out.println(query);
			String result = link.evaluateToOutputForm(query, 0);
			result = result.replaceAll("[, ]*False[, ]*", "");
			// if empty result then this rho is remote
			if (result.equals("{}")) {
				//System.out.println("Empty rho, removing");
				entry.getKey().setRemote();
				continue;
			}
			// add accesses to its corresponding table entry
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
		System.out.println("Printing table weight for vertex: " + this.vertexID);
		for (Map.Entry<String, ArrayList<String>> entry: tableAccesses.entrySet()) {
			if (Integer.valueOf(entry.getKey()) > 9) continue;
			String query = "";
			for (String access: entry.getValue()) {
				query += access + ", ";
			}
			query = query.substring(0, query.length() - 2);
			// size given by union of all the accesses, no duplicates
			String mathQuery = "Length[DeleteDuplicates[Union[" + query + "]]]";
			String result = link.evaluateToOutputForm(mathQuery, 0);
			System.out.println("Table: " + entry.getKey() + " Weight: " + result);
			
			vertexWeight += Integer.parseInt(result);
		}	
	}
	
	// method that checks if rho becomes remote after an update
	public void checkRemoteAfterUpdate(VertexRho rho, VertexPhi phi) {
		// get mathematica endpoint
		KernelLink link = MathematicaHandler.getInstance(); 
		
		String phiQuery = phi.getPhiAsGroup();
		String rhoQuery = rho.getRho().substring(rho.getRho().indexOf(">") + 1);
		// check if rho is constrained by logical subtraction
		if (rho.getRhoUpdate() != null) {
			rhoQuery = "(" + rhoQuery +  ") && (" + rho.getRhoUpdate() + ")";
		}
		// check items accessed by rho given phi
		String query = "Flatten[Table[" + rhoQuery + ", " + phiQuery + "]]";
		String result = link.evaluateToOutputForm(query, 0);
		// check if any items left on rho
		if (countMatches(result, "True") == 0) {
			//System.out.println("Empty rho, removing");
			rho.setRemote();
		}
	}
	
	// methos used to count how many times a substring appears in a string
	private int countMatches(String str, String query) {
		int lastIndex = 0;
		int count = 0;
		
		while(lastIndex != -1){

		    lastIndex = str.indexOf(query,lastIndex);

		    if(lastIndex != -1){
		        count ++;
		        lastIndex += query.length();
		    }
		}
		return count;
	}
	
	// debug and presentation print
	public void printVertex() {
		System.out.println("--------------------------");
		System.out.println("Printing vertex id: " + this.vertexID);
		System.out.println("Vertex weight: " + this.vertexWeight);
		this.sigma.printSigma();
		System.out.println("--------------------------");

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((sigma == null) ? 0 : sigma.hashCode());
		result = prime * result + txProfile;
		result = prime * result + vertexID;
		result = prime * result + vertexWeight;
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
		GraphVertex other = (GraphVertex) obj;
		if (sigma == null) {
			if (other.sigma != null)
				return false;
		} else if (!sigma.equals(other.sigma))
			return false;
		if (txProfile != other.txProfile)
			return false;
		if (vertexID != other.vertexID)
			return false;
		if (vertexWeight != other.vertexWeight)
			return false;
		return true;
	}
	
	
}