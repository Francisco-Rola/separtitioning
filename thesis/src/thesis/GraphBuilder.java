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
		Matcher m = Pattern.compile("\\w+_id").matcher(rho1);
		while(m.find()) {
			variables.add(rho1.substring(m.start(), m.end()));
			//System.out.println(rho1.substring(m.start(), m.end()));
		}
		m = Pattern.compile("\\w+_id").matcher(rho2);
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
	
	private static String rhoIntersection(String rhoV, String rhoGV, String phiV, String phiGV, KernelLink link) {

		if (rhoV.substring(0, rhoV.indexOf(">") - 1).equals(rhoGV.substring(0, rhoGV.indexOf(">") - 1))) {
			String rhoQuery = rhoV.substring(rhoV.indexOf(">") + 1) + " == " + rhoGV.substring(rhoGV.indexOf(">") + 1);
			String phiQuery = phiV + " && " + phiGV;
			String variables = findVariables(rhoV, rhoGV);
			String query = "Reduce[" + rhoQuery + " && " + phiQuery + ", " 
					+ variables + "]";
			System.out.println(query);
			String result = link.evaluateToOutputForm(query, 0);
			System.out.println(result);
			return result;
		}
		return "False";
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
	
	private static boolean logicalAdd(VertexSigma v, KernelLink link) {
		
		ArrayList<GraphEdge> foundEdges = new ArrayList<>();
		
		GraphVertex newVertex = new GraphVertex(v);
		
		// need to compare newly added vertex to every exisiting vertex
		HashMap<String, String> rhosV = v.getRhos();
		for (Map.Entry<GraphVertex, ArrayList<GraphEdge>> node : graph.entrySet()) {
			GraphVertex gv = node.getKey();
			// obtain all rhos in previously exisiting vertex
			HashMap<String, String> rhosGV = gv.getSigma().getRhos();
			// iterate through the new rhos being added
			for (Map.Entry<String, String> entryV: rhosV.entrySet()) {
				String rhoV = entryV.getKey();
				String phiV = entryV.getValue();
				for (Map.Entry<String, String> entryGV: rhosGV.entrySet()) {
					String rhoGV = entryGV.getKey();
					String phiGV = entryGV.getValue();
					
					//compute intersection between rhos given the phis
					String result = rhoIntersection(rhoV, rhoGV, phiV, phiGV, link);
					// check the intersection results
					if (result.equals("False")) {
						// no overlap so no subtraction needed
						continue;
					}
					else {
						System.out.println("Collision found, adding edge");
						// collision found, perform phi logical subtraction
						newVertex.getSigma().updatePhi(rhoV, result);
						// add edge between vertices whose rhos-phi overlapped
						GraphEdge edgeSrcV = new GraphEdge(newVertex, gv,rhoV, phiV);
						GraphEdge edgeSrcGV = new GraphEdge(gv, newVertex, rhoGV, phiGV, edgeSrcV.getEdgeWeight());
						foundEdges.add(edgeSrcV);
						foundEdges.add(edgeSrcGV);
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
		
		System.out.println("Vertex added successfully");
		return true;
	}
	
	private static void buildGraph(KernelLink link) {
		try {
			String[] files = {"payment_final.txt", "new_order_final.txt"};			
			new Parser(files);
			// After obtaining vertices from SE need to make them disjoint
			ArrayList<Vertex> seVertices = Parser.getVertices();
			
			boolean isFirst = true;
			
			for (Vertex v: seVertices) {
				if (isFirst) {
					// first vertex doesn't need subtracting
					isFirst = false;
					VertexSigma sigma = new VertexSigma(v.getRhos());
					GraphVertex gv = new GraphVertex(sigma);
					graph.put(gv, new ArrayList<>());
					System.out.println("First vertex added successfully");
					continue;
				}
				VertexSigma sigma = new VertexSigma(v.getRhos());
				logicalAdd(sigma, link);
			}
		} catch (IOException e1) {
			System.out.println("Unable to parse through SE files");
			e1.printStackTrace();
		}
	}

	public static void main(String[] args) {
		
		System.out.println("Running graph builder");
		
		KernelLink link = MathematicaHandler.getInstance();
		
		buildGraph(link);

	}
}