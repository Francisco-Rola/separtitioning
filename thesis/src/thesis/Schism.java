package thesis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;

public class Schism {
		
	public static void parseTrace(String graphFile, int numParts) {
		
		LinkedHashMap<String, LinkedHashMap<String, Integer>> metisGraph = new LinkedHashMap<>();
		
		HashMap<String, String> keyToID = new HashMap<>();
		
		HashMap<String, String> IDToKey = new HashMap<>();
		
		int id = 1;
		int noVertices = 0;
		int noEdges = 0;
		
		try (BufferedReader br = new BufferedReader(new FileReader("schism.txt"))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		       String[] items = line.split(" ");
		       
		       for (int i = 0; i < items.length; i++) {
		    	   String itemi = items[i];
		    	   
		    	   // check if this key has been seen before, if not generate new unique ID
		    	   if (!keyToID.containsKey(itemi)) {
		    		   keyToID.put(itemi, String.valueOf(id));
		    		   IDToKey.put(String.valueOf(id), itemi);
		    		   // initialize metis graph entry
		    		   LinkedHashMap<String, Integer> edges = new LinkedHashMap<>();
		    		   metisGraph.put(String.valueOf(id), edges);
		    		   id++;
		    		   noVertices++;
		    	   }
		    	   
		    	   String itemIDi = keyToID.get(itemi);
	
		    	   for (int j = 0; j < items.length; j++) {
		    		   if (i == j) continue;
		    		   
		    		   String itemj = items[j];
		    		   
		    		   // check if this key has been seen before, if not generate new unique ID
			    	   if (!keyToID.containsKey(itemj)) {
			    		   keyToID.put(itemj, String.valueOf(id));
			    		   IDToKey.put(String.valueOf(id), itemj);
			    		   // initialize metis graph entry
			    		   LinkedHashMap<String, Integer> edges = new LinkedHashMap<>();
			    		   metisGraph.put(String.valueOf(id), edges);
			    		   id++;
			    		   noVertices++;
			    	   }
			    	   
			    	   String itemIDj = keyToID.get(itemj);
			    	   
			    	   if (metisGraph.get(itemIDi).containsKey(itemIDj)) {
			    		   // old weight between i and j
			    		   int oldWeight = metisGraph.get(itemIDi).get(itemIDj);
			    		   // update i-j weight
			    		   metisGraph.get(itemIDi).put(itemIDj, oldWeight + 1);
			    		   // update j-i weight
			    		   metisGraph.get(itemIDj).put(itemIDi, oldWeight + 1);
			    	   }
			    	   else {
			    		   metisGraph.get(itemIDi).put(itemIDj, 1);
			    		   metisGraph.get(itemIDj).put(itemIDi, 1);
			    		   noEdges++;
			    	   }
		    	   }
		       }
		    }
		} catch (FileNotFoundException e) {
			System.out.println("Schism trace not found!");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Couldn't parse trace file!");
			e.printStackTrace();
		}		
		// create METIS file
		File metis = new File("metis.txt");
		try {
			if (metis.createNewFile()) {
			    System.out.println("File created: " + metis.getName());
			  } else {
			    System.out.println("File already exists.");
			  }
		} catch (IOException e) {
			System.out.println("Error while creating METIS file!");
			e.printStackTrace();
		}
		// write to METIS file
		try {
			FileWriter metisFile = new FileWriter("metis.txt");
			// first line contains no vertices, no edges and type of graph
			metisFile.append(noVertices + " " + noEdges + " " + "001\n");
			// iterate through all the vertices in the graph
			for (int i = 1; i <= noVertices; i++) {
				String line = "";
				for (Map.Entry<String, Integer> edge : metisGraph.get(String.valueOf(i)).entrySet()) {
					// get edge destination
					String edgeDest = edge.getKey();
					// get edge weight
					int edgeWeight = edge.getValue();
					
					line += edgeDest + " " + edgeWeight + " ";
				}
		        line = line.substring(0, line.length() - 1);
				// print new line and go to next vertex
				metisFile.append(line + "\n");
			}
			// close file
			metisFile.close();
			System.out.println("Successfully generated METIS file");
		} catch (IOException e) {
			System.out.println("Error on generating METIS file!");
			e.printStackTrace();
		}
				
		// command to run METIS 
		String command = "gpmetis " + graphFile  + " " + numParts;
		
		// map that stores partition for each data item
		HashMap<Integer, Integer> partitionMap = new HashMap<>();
		
		try {
			Runtime.getRuntime().exec(command);
			// output goes to a file
			File metisOut = new File(graphFile + ".part."+ numParts);
			metisOut.createNewFile();
			Scanner myReader = new Scanner(metisOut);
			// counter for each data item identifier
			int dataItemID = 0;
			while (myReader.hasNextLine()) {
				dataItemID++;
				String data = myReader.nextLine();
				partitionMap.put(dataItemID, Integer.parseInt(data));
			}
		    myReader.close();
		} catch (IOException e) {
			System.out.println("Error while runnning METIS command");
			e.printStackTrace();
		}
				
		// create WEKA Data file
		File dataFile = new File("schism.arff");
		try {
			if (dataFile.createNewFile()) {
			    System.out.println("File created: " + dataFile.getName());
			  } else {
			    System.out.println("File already exists.");
			  }
		} catch (IOException e) {
			System.out.println("Error while creating WEKA data file!");
			e.printStackTrace();
		}
		
		// write to WEKA data file
		try {
			FileWriter dataFileW = new FileWriter("schism.arff");
			// build feature file, ID,W,D,C,P
			
			dataFileW.append("@relation \'parts\'\n");
			dataFileW.append("@attribute w numeric\n");
			dataFileW.append("@attribute d numeric\n");
			dataFileW.append("@attribute c numeric\n");
			
			String partClass = "{";
			for (int i = 0; i < numParts; i++) {
				partClass = partClass + i + ",";
			}
			partClass = partClass.substring(0, partClass.length() - 1);
			partClass += "}";
			
			dataFileW.append("@attribute class " + partClass + "\n");
			
			dataFileW.append("@data\n");			
			
			for (Map.Entry<Integer, Integer> entry: partitionMap.entrySet()) {
				// obtained formatted key
				String key = IDToKey.get(String.valueOf(entry.getKey()));
				// obtain each of the features from key
				String w = "?";
				String d = "?";
				String c = "?";
				if (key.contains("w")) {
					w = "";
					int beginIndex = key.indexOf("w");
					for (int i = beginIndex + 1; i < key.length(); i++) {
						if (Character.isDigit(key.charAt(i))) {
							w += key.charAt(i);
						}
						else break;
					}
				}
				if (key.contains("d")) {
					d = "";
					int beginIndex = key.indexOf("d");
					for (int i = beginIndex + 1; i < key.length(); i++) {
						if (Character.isDigit(key.charAt(i))) {
							d += key.charAt(i);
						}
						else break;
					}
				}
				if (key.contains("c")) {
					c = "";
					int beginIndex = key.indexOf("c");
					for (int i = beginIndex + 1; i < key.length(); i++) {
						if (Character.isDigit(key.charAt(i))) {
							c += key.charAt(i);
						}
						else break;
					}
				}
				String dataLine = w + "," + d + "," + c + "," + entry.getValue() + "\n"; 
				dataFileW.append(dataLine);
			}
			dataFileW.close();
			System.out.println("Successfully generated WEKA data file");
		} catch (IOException e) {
			System.out.println("Error on generating WEKA data file!");
			e.printStackTrace();
		}
		
	

	}
	
	public static void main(String[] args) {
		
		TPCCWorkloadGenerator.buildSchismTrace(100);
		parseTrace("metis.txt", 10);

	}

}
