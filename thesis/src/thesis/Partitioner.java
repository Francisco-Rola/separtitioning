package thesis;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Scanner;

// class responsible for partitioning graph and presenting output
public class Partitioner {
	
	// no partitions desired 
	private static int noParts = 2;
	
	// graph under partitioning
	private LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph;
	
	// default constructor for partitioner, receives a graph and stores it
	public Partitioner(LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph) {
		this.graph = graph;
	}

	// method that invokes METIS to partition graph and prints result
	public static void partitionGraph() {
		// command to run METIS 
		String command = "gpmetis metis.txt " + noParts;
		// run the command
        try {
			Process proc = Runtime.getRuntime().exec(command);
			// output goes to a file
			File metisOut = new File("metis.txt.part."+ noParts);
		      Scanner myReader = new Scanner(metisOut);
		      while (myReader.hasNextLine()) {
		        String data = myReader.nextLine();
		        System.out.println(data);
		      }
		      myReader.close();
		} catch (IOException e) {
			System.out.println("Error while runnning METIS command");
			e.printStackTrace();
		}

	}
	
	// main for result analysis
	public static void main(String[] args) {
		// run partitioner
		partitionGraph();
	}
}
