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
	public void computeVertexWeightSMT(int vertexWeight) { 	
		// cannot be less than 0, if so its because of an over aproximation, fix it
		if (vertexWeight < 0) {
			this.vertexWeight = 0;
		}
		else {
			this.vertexWeight = vertexWeight;
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