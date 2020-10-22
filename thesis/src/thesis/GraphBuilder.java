package thesis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import com.wolfram.jlink.*;

public class GraphBuilder {
		
	private static LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph = new LinkedHashMap<>();
	
	private static String preparePhi(String phi, String update) {
		String preparedPhi = new String(phi);
		if (update != null) {
			// if there is a constraint on rho, phi needs an update
			 preparedPhi += " && " + update;
		}
		return preparedPhi;
	}
	
	private static String rhoIntersection(String rho1, String rho2, String phi1, String phi2, HashSet<String> vars1, HashSet<String> vars2) {
		KernelLink link = MathematicaHandler.getInstance();
		
		rho2 = variableRename(rho2);
		phi2 = variableRename(phi2);
		
		String rhoQuery = rho1.substring(rho1.indexOf(">") + 1) + " == " + rho2.substring(rho2.indexOf(">") + 1);
		String phiQuery = phi1 + " && " + phi2;
		
		// build variables string for mathematica query
		String variables = "{";
		for (String variable: vars1) {
			variables += variable + ", ";
		}
		for (String variable: vars2) {
			variables += variable.replaceAll("id", "idGV") + ", ";
		}
		// remove extra characters and finalize string
		variables = variables.substring(0, variables.length() - 2) + "}";

		
		String query = "Reduce[" + rhoQuery + " && " + phiQuery + ", " 
				+ variables + ", Integers, Backsubstitution -> True]";
		String result = link.evaluateToOutputForm(query, 0);
		
		return result;
	}
	
	private static String variableRename(String s) {
		return s.replaceAll("id", "idGV");
	}
	
	private static void addEdge(GraphEdge edge, LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph) {
		GraphVertex src = edge.getSrc();		
		if (graph.get(src) == null) {
			ArrayList<GraphEdge> edges = new ArrayList<>();
			edges.add(edge);
			graph.put(src, edges);
			return;
		}
		graph.get(src).add(edge);
	}
	
	public static void logicalAdd(GraphVertex newVertex, LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph) {
		
		ArrayList<GraphEdge> foundEdges = new ArrayList<>();
		
		// need to compare newly added vertex to every other vertex
		HashMap<VertexRho, VertexPhi> rhosV = newVertex.getSigma().getRhos();
		for (Map.Entry<GraphVertex, ArrayList<GraphEdge>> node : graph.entrySet()) {
			GraphVertex gv = node.getKey();
			// obtain all rhos in previously exisiting vertex
			HashMap<VertexRho, VertexPhi> rhosGV = gv.getSigma().getRhos();
			// iterate through the new rhos being added
			for (Map.Entry<VertexRho, VertexPhi> entryV: rhosV.entrySet()) {
				// don't compare remote rhos as they are elsewhere
				if (entryV.getKey().isRemote())
					continue;
				
				String rhoV = entryV.getKey().getRho();
				String phiV = entryV.getValue().getPhiAsString();
				
				for (Map.Entry<VertexRho, VertexPhi> entryGV: rhosGV.entrySet()) {
					String rhoGV = entryGV.getKey().getRho();
					String phiGV = entryGV.getValue().getPhiAsString();
					
					if (!rhoV.substring(0, rhoV.indexOf(">") - 1).equals(rhoGV.substring(0, rhoGV.indexOf(">") - 1))
							|| entryGV.getKey().isRemote())
						continue;
					
					//compute intersection between rhos given the phis
					String result = null;
					String phiVQ = preparePhi(phiV, entryV.getKey().getRhoUpdate());
					String phiGVQ = preparePhi(phiGV, entryGV.getKey().getRhoUpdate());
					result = rhoIntersection(rhoV, rhoGV, phiVQ, phiGVQ, entryV.getKey().getVariables(), entryGV.getKey().getVariables());
					// check the intersection results
					if (result.equals("False")) {
						// no overlap so no subtraction needed
						continue;
					}
					else {
						System.out.println("Collision found, adding edge");
						// collision found, perform rho logical subtraction
						entryV.getKey().updateRho(result);
						// add edge between vertices whose rhos-phi overlapped
						GraphEdge edgeSrcV = new GraphEdge(newVertex, gv,rhoV, rhoGV, entryV.getKey().getRhoUpdate(), entryV.getValue().getPhiAsGroup());
						foundEdges.add(edgeSrcV);
					}
				}
			}
		}
		// in this stage the new vertex has been compared and updated regarding all previous vertices
		graph.put(newVertex, null);
		
		// add its edges to the graph as well
		for (GraphEdge e : foundEdges) {
			addEdge(e, graph);
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
			// id for tx profle
			int txProfile = 1;
			
			for (Vertex v: seVertices) {
				if (isFirst) {
					isFirst = false;
					// build vertex sigma to identify items in V
					VertexSigma sigma = new VertexSigma(v.getRhos());
					// build a graph vertex 
					GraphVertex gv = new GraphVertex(sigma, txProfile);
					// store it in the graph
					graph.put(gv, new ArrayList<>());
					// compute its weight, i.e. how many items does it contain
					gv.computeVertexWeight();
					System.out.println("First vertex added successfully");
					continue;
				}
				VertexSigma sigma = new VertexSigma(v.getRhos());
				GraphVertex newVertex = new GraphVertex(sigma, txProfile);
				logicalAdd(newVertex, graph);
				txProfile++;
			}
		} catch (IOException e1) {
			System.out.println("Unable to parse through SE files");
			e1.printStackTrace();
		}
	}
	
	public static void printGraph(LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph) {
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
				
		Splitter splitter = new Splitter();
		
		graph = splitter.splitGraph(graph);
		
		printGraph(graph);

	}
}