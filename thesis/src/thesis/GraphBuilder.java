package thesis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/// class responsible for building the initial graph and invoking the split
public class GraphBuilder {
	// initial graph built from SE information	
	private static LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph = new LinkedHashMap<>();
	
	// method that builds graph from SE files
	private static void buildGraph() {
		try {
			// obtain vertices from SE tree
			String[] files = {"payment_final.txt", "new_order_final.txt"};			
			new Parser(files);
			// after obtaining vertices from SE need to make them disjoint
			ArrayList<Vertex> seVertices = Parser.getVertices();
			// iterate through SE vertices to add them to graph
			for (Vertex v: seVertices) {
				// build vertex sigma to identify items in V
				VertexSigma sigma = new VertexSigma(v.getRhos());
				// build a graph vertex 
				GraphVertex gv = new GraphVertex(sigma, v.getTxProfile());
				// store it in the graph
				graph.put(gv, new ArrayList<>());
				// increment transaction profile id
				System.out.println("Vertex added successfully");
				continue;
			}
		} catch (IOException e1) {
			System.out.println("Unable to parse through SE files");
			e1.printStackTrace();
		}
	}
	
	// debug and presentation method
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

	// tools main
	public static void main(String[] args) {
		
		System.out.println("Running graph builder");
				
		buildGraph();
				
		Splitter splitter = new Splitter();
		
		graph = splitter.splitGraph(graph);
		
		//printGraph(graph);

	}
}