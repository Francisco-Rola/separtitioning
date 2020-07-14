package thesis;

import com.wolfram.jlink.KernelLink;

// class that represents a edge in the graph
public class GraphEdge {
	
	// source vertex for the edge, V
	private GraphVertex src;
	// destiny vertex for the edge, GV
	private GraphVertex dest;
	// number of inputs that cause a transaction to be executed remotely
	private int edgeWeight = 0;
	// formula on V that overlaps with a previously existing formula on GV for the given edgePhi
	private String edgeRho = null;
	// set of inputs that cause vertices to overlap hence causing an edge
	private String edgePhi = null;
	
	public GraphEdge(GraphVertex src, GraphVertex dest, String edgeRho, String intersection)  {
		this.src = src;
		this.dest = dest;
		this.edgeRho = edgeRho;
		this.edgePhi = intersection.replaceAll("(\\s&&\\s)?\\(\\w+_id\\)\\s\\S+\\s\\d+", "")
						.replaceAll("idV", "id");
		this.edgeWeight = computeEdgeWeight(intersection);
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
	
	public void printEdge() {
		System.out.println("Edge rho: " + this.edgeRho);
		System.out.println("Edge phi: " + this.edgePhi);
		System.out.println("Edge weight: " + this.edgeWeight);
	}
	
	private int computeEdgeWeight(String edgePhi) {
		// obtain a mathematica endpoint
		KernelLink link = MathematicaHandler.getInstance();
		// build a length query that calculates length of the overlapping phis
		String query = "Length["+ edgePhi + "]";
		// how many inputs generate remote access 
		String result = link.evaluateToOutputForm(query, 0);
		// parse the output to an integer
		return Integer.parseInt(result);
	}

}
