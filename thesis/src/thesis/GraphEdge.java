package thesis;

import com.wolfram.jlink.KernelLink;

// class that represents a edge in the graph
public class GraphEdge {
	
	// source vertex for the edge
	private GraphVertex src;
	// destiny vertex for the edge
	private GraphVertex dest;
	// number of inputs that cause a transaction to be executed remotely
	private int edgeWeight = 0;
	// rho on the source vertex (new vertex)
	private String rhoSrc = null;
	// rho on the destination vertex (older vertex)
	private String rhoDest = null;
	// set of inputs that cause vertices to overlap hence causing an edge (src inputs)
	private String edgePhi = null;
	// range of inputs
	private String phiRange = null;
	
	// default constructor for graph edge
	public GraphEdge(GraphVertex src, GraphVertex dest, String rhoSrc, String rhoDest, String intersection, String phiRange, double prob) {
		this.src = src;
		this.dest = dest;
		this.rhoSrc = rhoSrc;
		this.rhoDest = rhoDest;
		this.phiRange = phiRange;
		this.edgePhi = intersection;
		this.edgeWeight = computeEdgeWeight(intersection, phiRange, prob);
		System.out.println("Edge S:" + src.getVertexID() + " D:" + dest.getVertexID() +
				" T:" + rhoSrc.substring(0, rhoSrc.indexOf(">") - 1) +  " W:" +  this.edgeWeight);
	}
	
	// getter for vertex src
	public GraphVertex getSrc() {
		return src;
	}

	// getter for vertex dest
	public GraphVertex getDest() {
		return dest;
	}

	// getter for edge weight
	public int getEdgeWeight() {
		return this.edgeWeight;
	}
	
	// method that computes edge weight, i.e. how many inputs cause a remote access
	private  int computeEdgeWeight(String intersection, String phiRange, double prob) {
		// get mathematica link
		KernelLink link = MathematicaHandler.getInstance();
		// compute how many inputs are in the overlap
		String query = "Length[Flatten[Table[" + intersection + ", " + phiRange + "]]]";
		String result = link.evaluateToOutputForm(query, 0);
		int probConverted = (int) (prob * 100);
		return Integer.parseInt(result) * probConverted;
	}
	
	// debug and presentation purposes
	public void printEdge() {
		System.out.println("Edge rho: " + this.rhoSrc);
		System.out.println("Edge phi: " + this.edgePhi);
		System.out.println("Edge weight: " + this.edgeWeight);
	}
	
}
