package thesis;

import java.util.StringJoiner;


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
	
	public GraphEdge(GraphVertex src, GraphVertex dest, String edgeRho, String intersection, int weight, boolean inbound)  {
		this.src = src;
		this.dest = dest;
		this.edgeRho = edgeRho;
		this.edgePhi = computeEdgePhi(inbound, intersection);
		System.out.println(edgePhi);
		this.edgeWeight = weight;
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
	
	private String computeEdgePhi(boolean inbound, String intersection) {
		System.out.println(intersection);
		StringJoiner edgePhiUpdated = new StringJoiner(" || ");
		String[] phiParts = intersection.split(" \\|\\| ");
		boolean ps = false;
		for (String disjunction: phiParts) {
			StringJoiner conjunctionPhi = new StringJoiner(" && ");
			String[] conjunctionParts = disjunction.split(" && ");
			for (int i = 0; i < conjunctionParts.length; i++) {
				if (!conjunctionParts[i].contains("idV")) {
					conjunctionPhi.add(conjunctionParts[i]);
				}
				else if (conjunctionParts[i].contains("Integers")) {
					ps = true;
					if (inbound ) {
						conjunctionPhi.add(conjunctionParts[i].replaceAll("\\| \\w+idV", ""));
					}
					else {
						conjunctionPhi.add(conjunctionParts[i].replaceAll("\\| \\w+id", "")
								.replaceAll("idV", "id"));
					}
				}
			}
			if(!ps) {
				edgePhiUpdated.add("(" + conjunctionPhi.toString());
			}
			else {
				edgePhiUpdated.add(conjunctionPhi.toString() + ")");
			}
			ps = false;
		}
		this.edgePhi = edgePhiUpdated.toString();
		
		return edgePhi;
	}
	
	public void printEdge() {
		System.out.println("Edge rho: " + this.edgeRho);
		System.out.println("Edge phi: " + this.edgePhi);
		System.out.println("Edge weight: " + this.edgeWeight);
	}
	
}
