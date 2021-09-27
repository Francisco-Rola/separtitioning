package thesis;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;



// class responsible for partitioning graph and presenting output
public class Partitioner {
	
	// no partitions desired 
	private int noParts = 2;
	
	// graph under partitioning
	private LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph;
	
	// final merged graph, there should be as many vertices as partitions
	private LinkedHashMap<Integer, ArrayList<GraphVertex>> mergedGraph = new LinkedHashMap<>();
	
	// default constructor for partitioner, receives a graph and stores it
	public Partitioner(LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph) {
		this.graph = graph;
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
		        System.out.println(data);
		        for (Map.Entry<GraphVertex, ArrayList<GraphEdge>> gv : graph.entrySet()) {
		        	if (vertex == gv.getKey().getVertexID()) {
	 	        		int partNumber = Integer.parseInt(data);
		        		if (mergedGraph.containsKey(partNumber)) {
		        			mergedGraph.get(partNumber).add(gv.getKey());
		        		}
		        		else {
		        			ArrayList<GraphVertex> verticesInPart = new ArrayList<>();
		        			verticesInPart.add(gv.getKey());
		        			mergedGraph.put(partNumber, verticesInPart);
		        		}
		        	}
		        }
		        vertex++;
		      }
		      myReader.close();
		} catch (IOException e) {
			System.out.println("Error while runnning METIS command");
			e.printStackTrace();
		}
        // at this point we have the mergedGraph ready, need to merge formulas in vertices belonging to same part
        mergeGraphFormulas(mergedGraph);
        
        // generate file with association between keys and vertex
        generateAssociations(mergedGraph);

	}
	
	public void mergeGraphFormulas(LinkedHashMap<Integer, ArrayList<GraphVertex>> mergedGraph) {
		// merge the formulas if need be
	}
	
	public void generateAssociations(LinkedHashMap<Integer, ArrayList<GraphVertex>> mergedGraph) {
		// create a file for C5.0 names
		File names = new File("evolve.names");
		try {
			if (names.createNewFile()) {
			    System.out.println("File created: " + names.getName());
			  } else {
			    System.out.println("File already exists.");
			  }
		} catch (IOException e) {
			System.out.println("Error while creating Names file!");
			e.printStackTrace();
		}
		// write to C.50 names file
		try {
			FileWriter namesFile = new FileWriter("evolve.names");
			// first line contains name of the relation
			namesFile.append("Partition.\t|\n\n");
			namesFile.append("WarehouseId:\tcontinuous.\n");
			namesFile.append("DistrictId:\tcontinuous.\n");
			namesFile.append("CustomerId:\tcontinuous.\n");
			namesFile.append("StockLevel:\tcontinuous.\n");
			namesFile.append("NewOrder:\tcontinuous.\n");
			namesFile.append("Order:\tcontinuous.\n");
			namesFile.append("Orderline:\tcontinuous.\n");

			String partition = "Partition:\t";
			for (int i = 0; i < noParts; i++) {
				partition = partition + i + ",";
			}
			partition = partition.substring(0,partition.length() - 1);
			partition += ".";
			
			namesFile.append(partition);
			
			namesFile.close();
			
			// create a file for C5.0 data
			File data = new File("evolve.data");
			try {
				if (data.createNewFile()) {
				    System.out.println("File created: " + data.getName());
				  } else {
				    System.out.println("File already exists.");
				  }
			} catch (IOException e) {
				System.out.println("Error while creating Data file!");
				e.printStackTrace();
			}
			// write to C.50 data file
			FileWriter dataFile = new FileWriter("evolve.data");

			// scaling factors
			int noWarehouses = VertexPhi.getScalingFactorW();
			int noDistricts = VertexPhi.getScalingFactorD();
			// generate table 1 -> warehouse keys
			for (int w_id = 0; w_id < noWarehouses; w_id++) {
				// lookup this key, find which vertex it belongs to
				int keyValue = w_id;
				String table = "1";
				String key = String.valueOf(keyValue);
				int vertex = lookupKey(key, table, mergedGraph);
				String c50Line = buildC50Line(String.valueOf(w_id), null, null, null, null, null, null,
						String.valueOf(vertex));
				dataFile.append(c50Line);
			}
			// generate table 2 -> district keys
			for (int w_id = 0; w_id < noWarehouses; w_id++) {
				for (int d_id = 0; d_id < noDistricts; d_id++) {
					int keyValue = (w_id * 100) + d_id;
					String table = "2";
					String key = String.valueOf(keyValue);
					int vertex = lookupKey(key, table, mergedGraph);
					String c50Line = buildC50Line(String.valueOf(w_id), String.valueOf(d_id), null, 
							null, null, null, null, String.valueOf(vertex));
					dataFile.append(c50Line);
				}
			}
			
			// close file
			dataFile.close();
			System.out.println("Successfully generated data file");
			
			
		} catch (IOException e) {
			System.out.println("Error on generating c50 file!");
			e.printStackTrace();
		}
	}
	
	// method to lookup vertex of warehouse key
	public int lookupKey(String key, String table, LinkedHashMap<Integer, ArrayList<GraphVertex>> mergedGraph) {
		// iterate over all partitions in mergedGraph
		for (Map.Entry<Integer, ArrayList<GraphVertex>> partition : mergedGraph.entrySet()) {
			// iterate over all vertices in each partition
			for (GraphVertex v : partition.getValue()) {
				// check whether v contains this key
				if (v.getSigma().containsKey(key, table)) {
					return partition.getKey().intValue();
				}
			}
		}
		return 0;
	}

	// method that writes a String for c50 file
	public String buildC50Line(String w, String d, String c, String s, String n, String o, String l, String p) {
		String line = "";
		// attributes
		line = (w != null) ? line + w + "," : line + "?,";
		line = (d != null) ? line + d + "," : line + "?,";
		line = (c != null) ? line + c + "," : line + "?,";
		line = (s != null) ? line + s + "," : line + "?,";
		line = (n != null) ? line + n + "," : line + "?,";
		line = (o != null) ? line + o + "," : line + "?,";
		line = (l != null) ? line + l + "," : line + "?,";
		// partition result is always there
		line = line + p + ".\n";
		return line;
	}

}
