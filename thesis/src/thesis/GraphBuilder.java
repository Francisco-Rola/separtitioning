package thesis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import com.wolfram.jlink.*;

public class GraphBuilder {
		
	private static HashMap<GraphVertex, ArrayList<GraphEdge>> graph = new HashMap<>();
	
	private static String findVariables(String rho1, String rho2) {
		HashSet<String> variables = new HashSet<>();
		Matcher m = Pattern.compile("\\w+idV\\w?").matcher(rho1);
		while(m.find()) {
			variables.add(rho1.substring(m.start(), m.end()));
			//System.out.println(rho1.substring(m.start(), m.end()));
		}
		m = Pattern.compile("\\w+id").matcher(rho2);
		while (m.find()) {
			variables.add(rho2.substring(m.start(), m.end()));
			//System.out.println(rho2.substring(m.start(), m.end()));
		}
		String variableList = "{";
		for (String variable: variables) {
			variableList += variable + ", ";
		}
		
		variableList = variableList.substring(0, variableList.length() - 2);
		// System.out.println(variableList + "}");
		return variableList + "}";
	}
	
	private static String rhoIntersection(String rho, String rhoGV, String phi, String phiGV, String update) {

		if (rho.substring(0, rho.indexOf(">") - 1).equals(rhoGV.substring(0, rhoGV.indexOf(">") - 1))) {
			
			KernelLink link = MathematicaHandler.getInstance();
			
			String rhoV = variableRename(rho);
			String phiV = variableRename(phi);
			
			String rhoQuery = rhoV.substring(rhoV.indexOf(">") + 1) + " == " + rhoGV.substring(rhoGV.indexOf(">") + 1);
			String phiQuery = phiV + " && " + phiGV;
			// check if rho is constrained by logical subtraction
			if (update != null) {
				phiQuery += " && !(" + update + ")"; 
			}
			String variables = findVariables(rhoV, rhoGV);
			String query = "Reduce[" + rhoQuery + " && " + phiQuery + ", " 
					+ variables + ", Integers, Backsubstitution -> True]";
			System.out.println(query);
			String result = link.evaluateToOutputForm(query, 0);
			
			return result;
		}
		return "False";
	}
	
	private static String variableRename(String s) {
		return s.replaceAll("id", "idV");
	}
	
	private static void addEdge(GraphEdge edge) {
		GraphVertex src = edge.getSrc();		
		if (graph.get(src) == null) {
			ArrayList<GraphEdge> edges = new ArrayList<>();
			edges.add(edge);
			graph.put(src, edges);
			return;
		}
		graph.get(src).add(edge);
	}
	
	private static void logicalAdd(VertexSigma v) {
		
		ArrayList<GraphEdge> foundEdges = new ArrayList<>();
		
		GraphVertex newVertex = new GraphVertex(v);
		
		// need to compare newly added vertex to every exisiting vertex
		HashMap<VertexRho, VertexPhi> rhosV = v.getRhos();
		for (Map.Entry<GraphVertex, ArrayList<GraphEdge>> node : graph.entrySet()) {
			GraphVertex gv = node.getKey();
			// obtain all rhos in previously exisiting vertex
			HashMap<VertexRho, VertexPhi> rhosGV = gv.getSigma().getRhos();
			// iterate through the new rhos being added
			for (Map.Entry<VertexRho, VertexPhi> entryV: rhosV.entrySet()) {
				String rhoV = entryV.getKey().getRho();
				String phiV = entryV.getValue().getPhiAsString();
				for (Map.Entry<VertexRho, VertexPhi> entryGV: rhosGV.entrySet()) {
					String rhoGV = entryGV.getKey().getRho();
					String phiGV = entryGV.getValue().getPhiAsString();
					
					//compute intersection between rhos given the phis
					String result = null;
					if (entryV.getKey().getRhoUpdate() != null) {
						result = rhoIntersection(rhoV, rhoGV, phiV, phiGV, entryV.getKey().getRhoUpdate());
					}
					else {
						result = rhoIntersection(rhoV, rhoGV, phiV, phiGV, null);
					}
					// check the intersection results
					if (result.equals("False")) {
						// no overlap so no subtraction needed
						continue;
					}
					else {
						System.out.println("Collision found, adding edge");
						// collision found, perform rho logical subtraction
						entryV.getKey().updateRho(result);
						// compute the size of the intersection
						String rhoVQuery = rhoV.substring(rhoV.indexOf(">") + 1);
						rhoVQuery += " && !(" + entryV.getKey().getRhoUpdate() + ")";
											
						// add edge between vertices whose rhos-phi overlapped
						GraphEdge edgeSrcV = new GraphEdge(newVertex, gv,rhoV, result, entryV.getValue().getPhiAsGroup());
						foundEdges.add(edgeSrcV);
					}
				}
			}
		}
		// in this stage the new vertex has been compared and updated regarding all previous vertices
		graph.put(newVertex, null);
		
		// add its edges to the graph as well
		for (GraphEdge e : foundEdges) {
			addEdge(e);
		}
		
		// compute vertex weight
		newVertex.computeVertexWeight();	
		
		System.out.println("Vertex added successfully");		
	}
	
	private static void buildGraph() {
		try {
			// obtain vertices from SE tree
			String[] files = {"payment_final.txt", "new_order_final.txt"};			
			new Parser(files);
			// After obtaining vertices from SE need to make them disjoint
			ArrayList<Vertex> seVertices = Parser.getVertices();
			// first vertex doesn't need special treatment
			boolean isFirst = true;
			
			for (Vertex v: seVertices) {
				if (isFirst) {
					isFirst = false;
					// build vertex sigma to identify items in V
					VertexSigma sigma = new VertexSigma(v.getRhos());
					// build a graph vertex 
					GraphVertex gv = new GraphVertex(sigma);
					// store it in the graph
					graph.put(gv, new ArrayList<>());
					// compute its weight, i.e. how many items does it contain
					gv.computeVertexWeight();
					System.out.println("First vertex added successfully");
					continue;
				}
				VertexSigma sigma = new VertexSigma(v.getRhos());
				logicalAdd(sigma);
			}
		} catch (IOException e1) {
			System.out.println("Unable to parse through SE files");
			e1.printStackTrace();
		}
	}
	
	public static void printGraph() {
		int noVertices = 1;
		for (Map.Entry<GraphVertex, ArrayList<GraphEdge>> entry: graph.entrySet()) {
			System.out.println("Printing vertex no " + noVertices);
			entry.getKey().printVertex();
			System.out.println("Printing edges for vertex no " + noVertices);
			for (GraphEdge e: entry.getValue()) {
				e.printEdge();
			}
			System.out.println("--------------------------");
			noVertices++;
		}
	}

	public static void main(String[] args) {
		
		System.out.println("Running graph builder");
				
		buildGraph();
		
		printGraph();
		
		new VertexSpliter(graph);

	}
}