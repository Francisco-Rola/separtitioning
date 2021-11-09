package thesis;

import java.time.Duration;
import java.time.Instant;

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
	public GraphEdge(int src, int dest, String rhoSrc, String intersection, String phiRange, double prob, int value) {
		this.src = src;
		this.dest = dest;
		this.rhoSrc = rhoSrc;
		this.edgePhi = intersection;
		this.edgeWeight = computeEdgeWeight(intersection, phiRange, prob) * value;
		System.out.println("Edge S:" + src + " D:" + dest+
				" T:" + rhoSrc.substring(0, rhoSrc.indexOf(">") - 1) +  " W:" +  this.edgeWeight);
	}
	
	// special constructor for different Mathematica format
	public GraphEdge(int src, int dest, String rhoSrc, int weight, double prob, int value) {
		this.src = src;
		this.dest = dest;
		this.rhoSrc = rhoSrc;
		this.edgePhi = "Mathematica invalid format";
		this.edgeWeight = weight * value;
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
		Instant start = Instant.now();
		String query = "Flatten[Table[" + intersection + ", " + phiRange + "]]";
		String result = link.evaluateToOutputForm(query, 0);
		Instant end = Instant.now();
		System.out.println("Edge weight computation time: " + Duration.between(start, end).toMillis());
		int noCollisions = countMatches(result, "False");
		int probConverted = (int) (prob * 1);
		return noCollisions * probConverted;
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
	
	// debug and presentation purposes
	public void printEdge() {
		System.out.println("Edge rho: " + this.rhoSrc);
		System.out.println("Edge phi: " + this.edgePhi);
		System.out.println("Edge weight: " + this.edgeWeight);
	}
	
}
