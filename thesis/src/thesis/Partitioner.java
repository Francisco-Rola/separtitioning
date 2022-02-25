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
	private int noParts = 2; 
	
	// graph under partitioning
	private LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph;
	
	// splitter used
	private NewSplitter splitter;
	
	// mapping between vertex and part
	private LinkedHashMap<GraphVertex, Integer> vertexPart = new LinkedHashMap<>();
	
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
           
        buildPartFunction();
	}
	
	// method that builds partitioning function from splits and metis results
	public void buildPartFunction() {
		LinkedHashMap<Integer,LinkedHashMap<String, Pair<Integer, Integer>>> splitRules = new LinkedHashMap<>();
		
		for (Map.Entry<GraphVertex, Integer> entry : vertexPart.entrySet()) {
			// get partition
			int part = entry.getValue().intValue();
			// get txProfile
			int txProfile = entry.getKey().getTxProfile();
			// get splits
			ArrayList<Split> splits = entry.getKey().getSigma().getSplits();
			// build rules
			ArrayList<String> rules = new ArrayList<>();
			for (Split split: splits) {
				String splitRule = "";
				String splitVar = split.getSplitParam();
				// check type of split
				if (splitVar.startsWith("#")) {
					// table split
					String table = splitVar.substring(1);
					splitVar = "key";
					
					splitRule += "if (table == " + table + " && (" +
					split.getLowerBound() + " <= key <= " + split.getUpperBound() + "))";
					
					rules.add(splitRule);
				}
				else {
					// input split
					splitRule += "if (" + split.getLowerBound() + " <= " + 
							splitVar + " <= " + split.getUpperBound() + "))";
					
					rules.add(splitRule);
				}	
			}
			for (String splitRule : rules) {
				// check if this txProfile has splitrules associated
				if (splitRules.containsKey(txProfile)) {
					// check if this splitrule is known
					if (splitRules.get(txProfile).containsKey(splitRule)) {
						// if a lower id vertex has a rule, that one should be kept
						if (splitRules.get(txProfile).get(splitRule).getKey() > entry.getKey().getVertexID()) {
							// update split rule if need be
							Pair<Integer, Integer> newRule = new Pair<>(entry.getKey().getVertexID(), part);
							splitRules.get(txProfile).put(splitRule, newRule);
						}
					}
					else {
						Pair<Integer, Integer> newRule = new Pair<>(entry.getKey().getVertexID(), part);
						splitRules.get(txProfile).put(splitRule, newRule);
					}
				}
				else {
					LinkedHashMap<String, Pair<Integer,Integer>> rulesForProfile = new LinkedHashMap<>();
					Pair<Integer, Integer> newRule = new Pair<>(entry.getKey().getVertexID(), part);
					rulesForProfile.put(splitRule, newRule);
					splitRules.put(txProfile, rulesForProfile);
				}
			}
		}
		
		// at this point we have collected all the splits per txProfile
		for (Map.Entry<Integer, LinkedHashMap<String , Pair<Integer, Integer>>> entry : splitRules.entrySet()) {
			System.out.println("TxProfile: " + entry.getKey());
			// print all the rules for given tx profile
			for (Map.Entry<String, Pair<Integer, Integer>> rule : entry.getValue().entrySet()) {
				System.out.println(rule.getKey());
				System.out.println("\tpart = " + rule.getValue().getValue() + ";\n");
			}
		}
	}
	
	// method that returns a partition given a vertex
	public int getPart(int vertex) {
		return vertexPart.get(vertex);
	}
	
	// method that starts the ML pipeline for Catalyst
	public void generateAssociations(LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph) {
		
		int dataThresh = 1000;
		int testThresh = 1000;
		
		try {
			
			
			// create a file for Weka training
			File data = new File("evolvetrain.arff");
			try {
				if (data.createNewFile()) {
				    System.out.println("File created: " + data.getName());
				  } else {
				    System.out.println("File already exists.");
				  }
			} catch (IOException e) {
				System.out.println("Error while creating train file!");
				e.printStackTrace();
			}
			// write to weka train file
			FileWriter dataFile = new FileWriter("evolvetrain.arff");
			dataFile.append("@relation 'parts'\n");
			dataFile.append("@attribute w numeric\n");
			dataFile.append("@attribute d numeric\n");
			dataFile.append("@attribute c numeric\n");
			dataFile.append("@attribute i numeric\n");
			dataFile.append("@attribute class {0,1}\n");
			dataFile.append("@data\n");
			
			// create a file for C5.0 data
			File test = new File("evolvetest.arff");
			try {
				if (test.createNewFile()) {
				    System.out.println("File created: " + test.getName());
				  } else {
				    System.out.println("File already exists.");
				  }
			} catch (IOException e) {
				System.out.println("Error while creating test file!");
				e.printStackTrace();
			}
			// write to weka test file
			FileWriter testFile = new FileWriter("evolvetest.arff");
			testFile.append("@relation 'parts'\n");
			testFile.append("@attribute w numeric\n");
			testFile.append("@attribute d numeric\n");
			testFile.append("@attribute c numeric\n");
			testFile.append("@attribute i numeric\n");
			testFile.append("@attribute class {0,1}\n");
			testFile.append("@data\n");

			// scaling factors
			int noWarehouses = VertexPhi.getScalingFactorW();
			int noDistricts = VertexPhi.getScalingFactorD();
			int noCustomers = VertexPhi.getScalingFactorC();
			int noItems = VertexPhi.getScalingFactorI();
			
			// random for thresholds
			int randomNum;
			
			// generate table 1 -> warehouse keys
			System.out.println("Generating table 1");
			for (int w_id = 0; w_id < noWarehouses; w_id++) {
				randomNum = ThreadLocalRandom.current().nextInt(0, 10000 + 1);
				if (randomNum <= dataThresh) {
					int keyValue = w_id;
					String table = "1";
					String key = String.valueOf(keyValue);
					int vertex = lookupKey(key, table);
					String c50Line = buildC50Line(String.valueOf(w_id), null, null, null,
							String.valueOf(vertex));
					dataFile.append(c50Line);
				}
				else if (randomNum <= testThresh){
					int keyValue = w_id;
					String table = "1";
					String key = String.valueOf(keyValue);
					int vertex = lookupKey(key, table);
					String c50Line = buildC50Line(String.valueOf(w_id), null, null, null,
							String.valueOf(vertex));
					testFile.append(c50Line);
				}
			}
			// generate table 2 -> district keys
			System.out.println("Generating table 2");
			for (int w_id = 0; w_id < noWarehouses; w_id++) {
				for (int d_id = 0; d_id < noDistricts; d_id++) {
					randomNum = ThreadLocalRandom.current().nextInt(0, 10000 + 1);
					if (randomNum <= dataThresh) {
						int keyValue = (w_id * 100) + d_id;
						String table = "2";
						String key = String.valueOf(keyValue);
						int vertex = lookupKey(key, table);
						String c50Line = buildC50Line(String.valueOf(w_id), String.valueOf(d_id), null, 
								null, String.valueOf(vertex));
						dataFile.append(c50Line);
					}
					else if (randomNum <= testThresh){
						int keyValue = (w_id * 100) + d_id;
						String table = "2";
						String key = String.valueOf(keyValue);
						int vertex = lookupKey(key, table);
						String c50Line = buildC50Line(String.valueOf(w_id), String.valueOf(d_id), null, 
								null, String.valueOf(vertex));
						testFile.append(c50Line);
					}
				}
			}
			// generate table 3 -> customer keys
			System.out.println("Generating table 3");
			for (int w_id = 0; w_id < noWarehouses; w_id++) {
				for (int d_id = 0; d_id < noDistricts; d_id++) {
					for (int c_id = 0; c_id < noCustomers; c_id++) {
						randomNum = ThreadLocalRandom.current().nextInt(0, 10000 + 1);
						if (randomNum <= dataThresh) {
							int keyValue = (w_id * 100) + d_id + (c_id * 10000);
							String table = "3";
							String key = String.valueOf(keyValue);
							int vertex = lookupKey(key, table);
							String c50Line = buildC50Line(String.valueOf(w_id), String.valueOf(d_id), String.valueOf(c_id), 
									null, String.valueOf(vertex));
							dataFile.append(c50Line);
						}
						else if (randomNum <= testThresh){
							int keyValue = (w_id * 100) + d_id + (c_id * 10000);
							String table = "3";
							String key = String.valueOf(keyValue);
							int vertex = lookupKey(key, table);
							String c50Line = buildC50Line(String.valueOf(w_id), String.valueOf(d_id), String.valueOf(c_id), 
									null, String.valueOf(vertex));
							testFile.append(c50Line);
						}
					}
				}
			}
			// generate table 5 -> new order keys
			System.out.println("Generating table 5");
			for (int w_id = 0; w_id < noWarehouses; w_id++) {
				for (int d_id = 0; d_id < noDistricts; d_id++) {
					for (int o_id = 0; o_id < noCustomers; o_id++) {
						randomNum = ThreadLocalRandom.current().nextInt(0, 1000 + 1);
						if (randomNum <= dataThresh) {
							int keyValue = (w_id * 100) + d_id + (o_id * 10000);
							String table = "5";
							String key = String.valueOf(keyValue);
							int vertex = lookupKey(key, table);
							String c50Line = buildC50Line(String.valueOf(w_id), String.valueOf(d_id), 
									String.valueOf(o_id), null, String.valueOf(vertex));
							dataFile.append(c50Line);
						}
						else if (randomNum <= testThresh){
							int keyValue = (w_id * 100) + d_id + (o_id * 10000);
							String table = "5";
							String key = String.valueOf(keyValue);
							int vertex = lookupKey(key, table);
							String c50Line = buildC50Line(String.valueOf(w_id), String.valueOf(d_id), 
									String.valueOf(o_id), null, String.valueOf(vertex));
							testFile.append(c50Line);
						}
					}
				}
			}
			// generate table 6 ->  order keys
			System.out.println("Generating table 6");
			for (int w_id = 0; w_id < noWarehouses; w_id++) {
				for (int d_id = 0; d_id < noDistricts; d_id++) {
					for (int o_id = 0; o_id < noCustomers; o_id++) {
						randomNum = ThreadLocalRandom.current().nextInt(0, 10000 + 1);
						if (randomNum <= dataThresh) {
							int keyValue = (w_id * 100) + d_id + (o_id * 10000);
							String table = "6";
							String key = String.valueOf(keyValue);
							int vertex = lookupKey(key, table);
							String c50Line = buildC50Line(String.valueOf(w_id), String.valueOf(d_id), 
									String.valueOf(o_id), null, String.valueOf(vertex));
							dataFile.append(c50Line);
						}
						else if (randomNum <= testThresh){
							int keyValue = (w_id * 100) + d_id + (o_id * 10000);
							String table = "6";
							String key = String.valueOf(keyValue);
							int vertex = lookupKey(key, table);
							String c50Line = buildC50Line(String.valueOf(w_id), String.valueOf(d_id), 
									String.valueOf(o_id), null, String.valueOf(vertex));
							testFile.append(c50Line);
						}
					}
				}
			}
			// generate table 7 -> order line keys
			System.out.println("Generating table 7");
			for (long w_id = 0; w_id < noWarehouses; w_id++) {
				for (long d_id = 0; d_id < noDistricts; d_id++) {
					for (long o_id = 0; o_id < noCustomers; o_id++) {
						for (long ol = 0; ol < 15; ol++) {
							randomNum = ThreadLocalRandom.current().nextInt(0, 10000 + 1);
							if (randomNum <= dataThresh) {
								long keyValue = (w_id * 100) + d_id + (o_id * 1000000) + (ol * 10000);
								String table = "7";
								String key = String.valueOf(keyValue);
								int vertex = lookupKey(key, table);
								String c50Line = buildC50Line(String.valueOf(w_id), String.valueOf(d_id), 
										String.valueOf(o_id), null, String.valueOf(vertex));
								dataFile.append(c50Line);
							}
							else if (randomNum <= testThresh){
								long keyValue = (w_id * 100) + d_id + (o_id * 1000000) + (ol * 10000);
								String table = "7";
								String key = String.valueOf(keyValue);
								int vertex = lookupKey(key, table);
								String c50Line = buildC50Line(String.valueOf(w_id), String.valueOf(d_id), 
										String.valueOf(o_id), null, String.valueOf(vertex));
								testFile.append(c50Line);
							}
						}
					}
				}
			}
			// generate table 8 -> item table
			System.out.println("Generating table 8");
			for (int i_id = 0; i_id < noItems; i_id++) {	
				randomNum = ThreadLocalRandom.current().nextInt(0, 10000 + 1);
				if (randomNum <= dataThresh) {
					int keyValue = i_id;
					String table = "8";
					String key = String.valueOf(keyValue);
					int vertex = lookupKey(key, table);
					String c50Line = buildC50Line(null, null, null, 
							String.valueOf(i_id), String.valueOf(vertex));
					dataFile.append(c50Line);
				}
				else if (randomNum <= testThresh){
					int keyValue = i_id;
					String table = "8";
					String key = String.valueOf(keyValue);
					int vertex = lookupKey(key, table);
					String c50Line = buildC50Line(null, null, null, 
							String.valueOf(i_id), String.valueOf(vertex));
					testFile.append(c50Line);
				}
			}
			// generate table 9 -> stock keys
			System.out.println("Generating table 9");
			for (int w_id = 0; w_id < noWarehouses; w_id++) {
				for (int i_id = 0; i_id < noItems; i_id++) {
					randomNum = ThreadLocalRandom.current().nextInt(0, 10000 + 1);
					if (randomNum <= dataThresh) {
						int keyValue = w_id + (i_id * 100);
						String table = "9";
						String key = String.valueOf(keyValue);
						int vertex = lookupKey(key, table);
						String c50Line = buildC50Line(String.valueOf(w_id), null, null, 
								String.valueOf(i_id), String.valueOf(vertex));
						dataFile.append(c50Line);
					}
					else if (randomNum <= testThresh){
						int keyValue = w_id + (i_id * 100);
						String table = "9";
						String key = String.valueOf(keyValue);
						int vertex = lookupKey(key, table);
						String c50Line = buildC50Line(String.valueOf(w_id), null, null, 
								String.valueOf(i_id), String.valueOf(vertex));
						testFile.append(c50Line);
					}
				}
			}
			
			
			// close file
			dataFile.close();
			testFile.close();
			System.out.println("Successfully generated data and test files");
			
			
		} catch (IOException e) {
			System.out.println("Error on generating weka file!");
			e.printStackTrace();
		}
	}
	
	// method to lookup vertex of warehouse key
	public int lookupKey(String key, String table) {
		// iterate over all vertices in graph
		for (Map.Entry<GraphVertex, ArrayList<GraphEdge>> entry : graph.entrySet()) {
			// check whether this vertex contains the key
			if (entry.getKey().getSigma().containsKey(key, table)) {
				return getPart(entry.getKey().getVertexID());
			}
		}
		System.out.println("Couldn't find key " + key + " on table " + table);
		return 0;
	}

	// method that writes a String for c50 file
	public static String buildC50Line(String w, String d, String c, String s, String n, String o, String l, String p) {
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
		line = line + p + "\n";
		return line;
	}
	
	// method that writes a String for c50 file
	public static String buildC50Line(String w, String d, String c, String o, String p) {
		String line = "";
		// attributes
		line = (w != null) ? line + w + "," : line + "?,";
		line = (d != null) ? line + d + "," : line + "?,";
		line = (c != null) ? line + c + "," : line + "?,";
		line = (o != null) ? line + o + "," : line + "?,";
		// partition result is always there
		line = line + p + "\n";
		return line;
	}
	
}
