package thesis;

import com.wolfram.jlink.KernelLink;

// class that represents a edge in the graph
public class GraphEdge {
	
	// source vertex for the edge
	private int src;
	// destiny vertex for the edge
	private int dest;
	// number of inputs that cause a transaction to be executed remotely
	private int edgeWeight = 0;
	// rho on the source vertex (new vertex)
	private String rhoSrc = null;
	// set of inputs that cause vertices to overlap hence causing an edge (src inputs)
	private String edgePhi = null;
	
	// default constructor for graph edge
	public GraphEdge(int src, int dest, String rhoSrc, String intersection, String phiRange, double prob) {
		this.src = src;
		this.dest = dest;
		this.rhoSrc = rhoSrc;
		this.edgePhi = intersection;
		this.edgeWeight = computeEdgeWeight(intersection, phiRange, prob);
		System.out.println("Edge S:" + src + " D:" + dest+
				" T:" + rhoSrc.substring(0, rhoSrc.indexOf(">") - 1) +  " W:" +  this.edgeWeight);
	}
	
	// getter for vertex src
	public int getSrc() {
		return src;
	}

	// getter for vertex dest
	public int getDest() {
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
