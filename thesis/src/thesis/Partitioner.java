package thesis;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;



// class responsible for partitioning graph and presenting output
public class Partitioner {
	
	// no partitions desired 
	private static int noParts = 2; 
	
	// graph under partitioning
	private LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph;
	
	// splitter used
	private NewSplitter splitter;
	
	// mapping between vertex and part
	private LinkedHashMap<GraphVertex, Integer> vertexPart = new LinkedHashMap<>();
	
	// map that given a tx profile gives associated rules to part mapping
	LinkedHashMap<Integer,LinkedHashMap<Split, Integer>> splitRules = new LinkedHashMap<>();
	
	// default constructor for partitioner, receives a graph and stores it
	public Partitioner(LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph, NewSplitter splitter) {
		this.graph = graph;
		this.splitter = splitter;
	}

	// method that invokes METIS to partition graph and prints result
	public void partitionGraph() {
		// command to run METIS
		String command = "gpmetis metis.txt " + noParts;
		// run the command
        try {
			Runtime.getRuntime().exec(command);
			// output goes to a file
			File metisOut = new File("metis.txt.part."+ noParts);
		      Scanner myReader = new Scanner(metisOut);
		      int vertex = 1;
		      while (myReader.hasNextLine()) {
		    	// each vertex gets an assigned partition
		        String data = myReader.nextLine();
		        // data contains part for vertex 
		        System.out.println(data);
		        for (Map.Entry<GraphVertex, ArrayList<GraphEdge>> entry : graph.entrySet()) {
		        	if (vertex == entry.getKey().getVertexID()) {
		        		 // store mapping between vertex and part
				        vertexPart.put(entry.getKey(), Integer.parseInt(data));
				        vertex++;
				        break;
		        	}		
		        }
		      }
		      myReader.close();
		} catch (IOException e) {
			System.out.println("Error while runnning METIS command");
			e.printStackTrace();
		}
        // build partitioning logic        
        buildPartLogic();
	}
	
	// method that creates partitioning logic
	public void buildPartLogic() {
		// each vertex will lead to partition rules due to its splits
		for (Map.Entry<GraphVertex, Integer> entry : vertexPart.entrySet()) {
			// get partition
			int part = entry.getValue().intValue();
			// get txProfile
			int txProfile = entry.getKey().getTxProfile();
			// get splits
			ArrayList<Split> splits = entry.getKey().getSigma().getSplits();
			// build map between rules and part
			for (Split split: splits) {
				if (splitRules.containsKey(txProfile)) {
					splitRules.get(txProfile).put(split, part);
				}
				else {
					LinkedHashMap<Split, Integer> ruleToPart = new LinkedHashMap<>();
					ruleToPart.put(split, part);
					splitRules.put(txProfile, ruleToPart);
				}			
			}
		}
		// print out the partitioning results
		for (Map.Entry<Integer, LinkedHashMap<Split, Integer>> profileRules : splitRules.entrySet()) {
			System.out.println("Tx profile: " + profileRules.getKey());
			// print out all rules for profile
			for (Map.Entry<Split, Integer> rules : profileRules.getValue().entrySet()) {
				rules.getKey().printRule(rules.getValue());
			}
		}
	}

	// methos that returns part logic computed
	public LinkedHashMap<Integer,LinkedHashMap<Split, Integer>> getPartLogic() {
		return this.splitRules;
	}
	
	// method that retrieves number of parts used by partitioner
	public static int getNoParts() {
		return noParts;
	}
}
