package thesis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/// class responsible for building the initial graph and invoking the split
public class GraphBuilder {
	// initial graph built from SE information	
	private static LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph = new LinkedHashMap<>();
	
	// scaling parameters
	public static int noW;
	public static int noP;
	
	// method that starts the graph building process, constructor
	public GraphBuilder(int workload, int noW, int noP) {
		
		// store parameters
		GraphBuilder.noW = noW;
		GraphBuilder.noP = noP;
		
		System.out.println("Running graph builder");
		
		// start the clock
		long startTimeTotal = System.currentTimeMillis();
		buildGraph(workload);
		long stopTimeGraphInitial = System.currentTimeMillis();
		
	    long elapsedTime = stopTimeGraphInitial - startTimeTotal;
	    
	    System.out.println("Time to build initial symbolic graph (pre splitting): " + elapsedTime);
	    
		printGraph(graph); 
		
		/*
		
		System.out.println("Running graph splitter");
		
		NewSplitter splitter = new NewSplitter();
		
		graph = splitter.splitGraph(graph);
				
		printGraph(graph);
		
		System.out.println("Running graph partitioner");
		
		long startTimePartitioning = System.currentTimeMillis();
		
		Partitioner partitioner = new Partitioner(graph, splitter);
		
		partitioner.partitionGraph();   
		
	    long stopTimePartitioning = System.currentTimeMillis();

	    elapsedTime = stopTimePartitioning - startTimePartitioning;
	    
	    System.out.println("Time to build partition graph and build logic: " + elapsedTime);

	    elapsedTime = stopTimePartitioning - startTimeTotal;
	    
	    System.out.println("\nTotal execution time: " + elapsedTime); 
	    
	    */
	    
	    //CatalystEvaluation evaluation = new CatalystEvaluation(partitioner);
	    
	    //evaluation.evaluateCatalyst(workload, noP);


	}
	
	// method that builds graph from SE files
	private static void buildGraph(int workload) {
		try {
			// TPCC
			if (workload == 1) {
				String[] files = {"payment_new.txt", "order_new.txt", "delivery_new.txt",
						"order_status_experiment.txt", "stock_level_experiment.txt"}; 
				//String[] files = {"payment_new.txt"};
				// obtain vertices from SE tree
				new SEParser(files);
			}
			// RUBIS
			else {
				String[] files = {"registeruser_experiment.txt", "registeritem_experiment.txt",
						"storebuy_experiment.txt", "storebid_experiment.txt","storecomment_experiment.txt"};
				// obtain vertices from SE tree
				new SEParser(files);
			}
			
			// after obtaining vertices from SE need to make them disjoint
			ArrayList<Vertex> seVertices = SEParser.getVertices();
			// iterate through SE vertices to add them to graph
			for (Vertex v: seVertices) {
				// build vertex sigma to identify items in V
				VertexSigma sigma = new VertexSigma(v.getRhos());
				// build a graph vertex 
				GraphVertex gv = new GraphVertex(sigma, v.getTxProfile());
				// store it in the graph
				graph.put(gv, new ArrayList<>());
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
	
	// build graph and feed it to splitter and partitioner
	public static void main(String[] args) {
		
		

		
		
	

						
		

	}
}