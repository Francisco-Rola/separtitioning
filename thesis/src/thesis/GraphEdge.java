package thesis;

import java.util.StringJoiner;

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
	// range of inputs
	private String phiRange = null;
	
	public GraphEdge(GraphVertex src, GraphVertex dest, String edgeRho, String intersection, String phiRange)  {
		this.src = src;
		this.dest = dest;
		this.edgeRho = edgeRho;
		this.phiRange = phiRange;
		this.edgePhi = intersection;
		System.out.println(edgePhi);
		this.edgeWeight = computeEdgeWeight(edgeRho, intersection, phiRange);
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
		KernelLink link = MathematicaHandler.getInstance();
		
		String rhoVQuery = rhoE.substring(rhoE.indexOf(">") + 1);
		rhoVQuery += " && !(" + phiE + ")";
		
		String query = "Length[Flatten[Table[" + rhoE + ", " + phiRange + "]]]";
		String result = link.evaluateToOutputForm(query, 0);
		return Integer.parseInt(result);
	}
	
	public void printEdge() {
		System.out.println("Edge rho: " + this.edgeRho);
		System.out.println("Edge phi: " + this.edgePhi);
		System.out.println("Edge weight: " + this.edgeWeight);
	}
	
}
