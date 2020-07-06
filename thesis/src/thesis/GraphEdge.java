package thesis;

public class GraphEdge {
	
	private GraphVertex src;
	private GraphVertex dest;
	
	private int edgeWeight = 0;
	
	private String edgeRho = null;
	private String edgePhi = null;
	
	public GraphEdge(GraphVertex src, GraphVertex dest, String edgeRho, String edgePhi)  {
		this.src = src;
		this.dest = dest;
		this.edgeRho = edgeRho;
		this.edgePhi = edgePhi;
		this.edgeWeight = computeEdgeWeight(edgePhi);		
	}
	
	public GraphEdge(GraphVertex src, GraphVertex dest, String edgeRho, String edgePhi, int weight)  {
		this.src = src;
		this.dest = dest;
		this.edgeRho = edgeRho;
		this.edgePhi = edgePhi;
		this.edgeWeight = weight;
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
	
	private int computeEdgeWeight(String edgePhi) {
		
		// how many inputs generate remote access 
		int weight = 0;
		
		String aux = edgePhi;
		
		int start = 0;
		int end = 0;
		
		
		while (aux != "") {
			start = Integer.parseInt(aux.substring(0, aux.indexOf("<=") - 1));
			aux = aux.substring(aux.indexOf("id < ") + 5);
			
			if (aux.contains("&&")) {
				end = Integer.parseInt(aux.substring(0, aux.indexOf(" ")));
				aux = aux.substring(aux.indexOf("&& ") + 3);

			}
			else {
				end = Integer.parseInt(aux);
				aux = "";
			}
			
			if (weight != 0 ) {
				weight *= end - start ;
			}
			else {
				weight += end - start;

			}

		}		
		return weight;
			
	}

}
