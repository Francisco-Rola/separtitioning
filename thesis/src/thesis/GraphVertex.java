package thesis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

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
	// consider transaction frequency
	boolean frequency = true;
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
	
	// transaction likelihood information
	private static double getTXLikelihood(int txProfile) {
		// payment
		if (txProfile == 1) {
			return 0.431;
		}
		else if (txProfile == 2) {
			return 0.445;
		}
		else if (txProfile == 3) {
			return 0.042;
		}
		else {
			System.out.println("Invalid transaction profile");
			return 0;
		}
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
			String phiQuery = entry.getValue().getPhiAsGroup();
			String rhoQuery = entry.getKey().getRho().substring(entry.getKey().getRho().indexOf(">") + 1);
			// check if rho is constrained by logical subtraction
			if (entry.getKey().getRhoUpdate() != null) {
				rhoQuery = "(" + rhoQuery +  ") && (" + entry.getKey().getRhoUpdate() + ")";
			}
			// check items accessed by rho given phi
			String query = "Flatten[Table[" + rhoQuery + ", " + phiQuery + "]]";
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
		for (Map.Entry<String, ArrayList<String>> entry: tableAccesses.entrySet()) {
			String query = "";
			for (String access: entry.getValue()) {
				query += access + ", ";
			}
			query = query.substring(0, query.length() - 2);
			// size given by union of all the accesses, no duplicates
			String mathQuery = "Length[DeleteDuplicates[Union[" + query + "]]]";
			String result = link.evaluateToOutputForm(mathQuery, 0);
			System.out.println("Table: " + entry.getKey() + " Weight: " + result);
			
			if (frequency) {
				int freqConverted = (int) (getTXLikelihood(this.txProfile) * 100);
				vertexWeight += Integer.parseInt(result) * freqConverted;
			}
			else 
				vertexWeight += Integer.parseInt(result);
		}
		
	}
	
	// debug and presentation print
	public void printVertex() {
		System.out.println("--------------------------");
		System.out.println("Printing vertex id: " + this.vertexID);
		System.out.println("Vertex weight: " + this.vertexWeight);
		this.sigma.printSigma();
		System.out.println("--------------------------");

	}
	
	// automatically generated hash code for vertex sigma
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((sigma == null) ? 0 : sigma.hashCode());
		result = prime * result + txProfile;
		result = prime * result + vertexWeight;
		return result;
	}
}