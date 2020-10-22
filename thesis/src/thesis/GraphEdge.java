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
	
	public GraphEdge(GraphVertex src, GraphVertex dest, String rhoSrc, String rhoDest, String intersection, String phiRange) {
		this.src = src;
		this.dest = dest;
		this.rhoSrc = rhoSrc;
		this.rhoDest = rhoDest;
		this.phiRange = phiRange;
		this.edgePhi = intersection;
		this.edgeWeight = computeEdgeWeight(rhoSrc, intersection, phiRange);
		System.out.println("Added edge with weight " + this.edgeWeight );
	}
	
	public GraphVertex getSrc() {
		return src;
	}

	public GraphVertex getDest() {
		return dest;
	}

	public int getEdgeWeight() {
		return this.edgeWeight;
	}
	
	private  int computeEdgeWeight(String rhoE, String phiE, String phiRange) {
		// get mathematica link
		KernelLink link = MathematicaHandler.getInstance();
		// compute how many inputs are in the overlap
		String query = "Length[Flatten[Table[" + phiE + ", " + phiRange + "]]]";
		String result = link.evaluateToOutputForm(query, 0);
		return Integer.parseInt(result);
	}
	
	public void printEdge() {
		System.out.println("Edge rho: " + this.rhoSrc);
		System.out.println("Edge phi: " + this.edgePhi);
		System.out.println("Edge weight: " + this.edgeWeight);
	}
	
}
