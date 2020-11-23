package thesis;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.wolfram.jlink.KernelLink;


// class performing vertex splitting and creating graph file for the partitioner
public class Splitter {
	
	// how many parts does a table split go for
	int tableSplitFactor = 2;
	// how many splits does an input split go for, at most
	int inputSplitFactor = 10;
	// no edges in final graph
	int noEdges = 0;
	// no vertices in final graph
	int noVertices = 0;
	// graph resultant from splitting
	private LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> splitGraph = new LinkedHashMap<>();
	
	// method that takes a graph as input and splits its vertices, returns the resulting graph
	public LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> splitGraph(LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph) {
		// for each vertex, iterate through its rhos
		int counter = 1;
		// tx profile identifier
		int txProfile = 1;
		// go through all vertices in graph
		for (GraphVertex v: graph.keySet()) {
			// obtain rho phi pair for the given vertex
			HashMap<VertexRho, VertexPhi> rhos = v.getSigma().getRhos();
			// group together rhos that belong to the same table within a vertex
			HashMap<Integer, ArrayList<Pair<VertexRho, VertexPhi>>> buckets = groupRhos(rhos);
			// after having rhos grouped by table, analyze each table and get possible splits
			HashMap<Integer, ArrayList<HashSet<String>>> possibleSplits = analyseTables(buckets);
			// get the shortest splitting variable set and table splits needed
			ArrayList<String> splits = getSplits(possibleSplits);
			// display the splits
			printSplits(counter, splits);
			// apply the split to the vertex
			applySplit(v.getSigma(), splits, txProfile);
			//increment vertex counter
			counter++;
		}
		// build metis graph
		LinkedHashMap<Pair<Integer, Integer>, HashMap<Integer, Integer>> metisGraph = buildMETISGraph();
		// print matrix
		printMatrix(metisGraph);
		// print METIS file
		printMETISfile(metisGraph);
		return splitGraph;
	}
	
	// method responsible for grouping together rhos on the same table
	private HashMap<Integer, ArrayList<Pair<VertexRho, VertexPhi>>> groupRhos(HashMap<VertexRho, VertexPhi> rhos) {
		// map between table identifier and rhos that access it
		HashMap<Integer, ArrayList<Pair<VertexRho, VertexPhi>>> buckets = new HashMap<>();
		// iterate over all rhos and check which table they access
		for (Map.Entry<VertexRho, VertexPhi> entry : rhos.entrySet()) {
			Pair<VertexRho, VertexPhi> rhoPhi = new Pair<VertexRho, VertexPhi>(entry.getKey(), entry.getValue());
			Integer tableNo = Integer.parseInt(rhoPhi.getKey().getRho().substring(0, rhoPhi.getKey().getRho().indexOf(">") - 1));
			// if buvcket exists for given table, add to it
			if (buckets.containsKey(tableNo)) {
				buckets.get(tableNo).add(rhoPhi);
			}
			// new table, create a bucket for it
			else {
				ArrayList<Pair<VertexRho, VertexPhi>> bucket = new ArrayList<>();
				bucket.add(rhoPhi);
				buckets.put(tableNo, bucket);
			}
		}
		return buckets;
	}
	
	// method responsible for analyzing bucket and determining how to split each table
	private HashMap<Integer, ArrayList<HashSet<String>>> analyseTables(HashMap<Integer, ArrayList<Pair<VertexRho, VertexPhi>>> buckets) {
		// variable that stores rho intersection information
		boolean stop = false;
		// possible split variables per tab
		HashMap<Integer, ArrayList<HashSet<String>>> possibleSplits = new HashMap<>();
		// go through all the buckets to analyze each table
		for (Map.Entry<Integer, ArrayList<Pair<VertexRho, VertexPhi>>> entry: buckets.entrySet()) {	
			// reset intersection flag
			stop = false;
			// list of variables that splits a table
			ArrayList<HashSet<String>> splits = new ArrayList<>();
			// check how many rhos for given table
			if (entry.getValue().size() == 1) {
				// only rho for table, must be split using one of its variables
				splits.add(entry.getValue().iterator().next().getKey().getVariables());
				possibleSplits.put(entry.getKey(), splits);
				continue;
			}
			for (int i = 0; i < entry.getValue().size(); i++) {
				// if an intersection was found on this table then it has been split
				if (stop) break;
				Pair<VertexRho, VertexPhi> rhoPhi1 = entry.getValue().get(i);
				for (int j = i + 1; j < entry.getValue().size(); j++) {
					Pair<VertexRho, VertexPhi> rhoPhi2 = entry.getValue().get(j);
					boolean intersection = checkIntersection(rhoPhi1, rhoPhi2);
					if (!intersection) {
						// rhos do not overlap, add all the variables that can split them
						splits.add(rhoPhi1.getKey().getVariables());
						splits.add(rhoPhi2.getKey().getVariables());
						possibleSplits.put(entry.getKey(), splits);
					}
					else {
						// check if there is a split variable common amongst all rhos
						HashSet<String> commonSplit = checkCommonSplit(entry.getValue());
						// check if any vars found
						if (commonSplit.size() != 0) {
							// split found, add it
							splits.add(commonSplit);
							possibleSplits.put(entry.getKey(), splits);
						}
						else {
							// rhos overlap for some input, splitting the table instead
							possibleSplits.remove(entry.getKey());
							// mark possible splits for table as null to know it is a table split
							possibleSplits.put(entry.getKey(), null);	
						}
						// set stop variable, indicating table is dealt with
						stop = true;
						break;
					}
				}
			}
		}
		return possibleSplits;
	}
	
	// method that checks if two rhos can intersect for any input
	private boolean checkIntersection(Pair<VertexRho, VertexPhi> rhoPhi1, Pair<VertexRho, VertexPhi> rhoPhi2) {
		// get rhos and phis as strings
		String rho1 = rhoPhi1.getKey().getRho();
		String rho2 = rhoPhi2.getKey().getRho();
		String phi1 = rhoPhi1.getValue().getPhiAsString();
		String phi2 = rhoPhi2.getValue().getPhiAsString();
		// rename vars for mathematica query
		rho2 = variableRename(rho2);
		phi2 = variableRename(phi2);
		
		String rhoQuery = rho1.substring(rho1.indexOf(">") + 1) + " == " + rho2.substring(rho2.indexOf(">") + 1);
		String phiQuery = phi1 + " && " + phi2;
		
		HashSet<String> vars1 = rhoPhi1.getKey().getVariables();
		HashSet<String> vars2 = rhoPhi2.getKey().getVariables();
		
		String variableList = "{";
		for (String variable: vars1) {
			variableList += variable + ", ";
		}
		for (String variable: vars2) {
			variableList += variable.replaceAll("id", "idGV") + ", ";
		}
		variableList = variableList.substring(0, variableList.length() - 2);
		variableList += "}";
		
		String query = "FindInstance[" + rhoQuery + " && " + phiQuery + ", " 
				+ variableList + ", Integers]";
		
		KernelLink link = MathematicaHandler.getInstance();
		String result = link.evaluateToOutputForm(query, 0);
		
		// check if there is an intersection
		if (result.equals("{}")) {
			return false;
		}
		else {
			return true;
		}
	}
	
	// method that leverages regex for var renaming
	private String variableRename(String s) {
		// rename variables to prepare mathematica query
		return s.replaceAll("id", "idGV");
	}
	
	// method that marks all the splits and gets the splitting variables set
	private ArrayList<String> getSplits(HashMap<Integer, ArrayList<HashSet<String>>> possibleSplits) {
		ArrayList<String> splits = new ArrayList<>();
		// map between rhoID and variables that cover it
		HashMap<Integer, HashSet<String>> rhoCoverage = new HashMap<>();
		// rho identifier
		int rhoID = 1;
		for (Map.Entry<Integer, ArrayList<HashSet<String>>> entry: possibleSplits.entrySet()) {
			if (entry.getValue() == null) {
				// table split detected, store it
				splits.add("#" + entry.getKey().toString());
				continue;
			}
			for (HashSet<String> splitters: entry.getValue()) {
				rhoCoverage.put(rhoID, splitters);
				rhoID++;
			}
		}
		// initial set of splitting vars is empty
		ArrayList<String> splitVars = new ArrayList<>();
		// get the smallest hitting set, NP complete problem
		splits.addAll(getSplitVars(rhoCoverage, splitVars, null));
		// return all splits, table splits and variables
		return splits;
	}
	
	// method that solves hitting set problem
	private ArrayList<String> getSplitVars(HashMap<Integer, HashSet<String>> rhoCoverage, ArrayList<String> splitVars, String splitVar) {
		if (splitVar != null) {
			// remove thos that are covered by var
			ArrayList<Integer> toRemove = new ArrayList<>();
			for (Map.Entry<Integer, HashSet<String>> rho: rhoCoverage.entrySet()) {
				if (rho.getValue().contains(splitVar)) 
					toRemove.add(rho.getKey());
			}
			
			for (Integer i: toRemove)
				rhoCoverage.remove(i);
		}
		// parameter that caps how many how many split vars a rho can have 
		int splitFactor = 1;
		while (!rhoCoverage.isEmpty()) {
			// obtain alternative splits considering rhos that can be split only by noAlternatives vars
			for (Map.Entry<Integer, HashSet<String>> entry: rhoCoverage.entrySet()) {
				if (entry.getValue().size() == splitFactor && splitFactor == 1) {
					// in this scenario this variable is the only one capable of splitting the rho		
					String split = entry.getValue().iterator().next();
					splitVars.add(split);
					//System.out.println("Must split variable found: " + split);
					return getSplitVars(rhoCoverage, splitVars, split);
				}
				else if (entry.getValue().size() == splitFactor && splitFactor > 1) {
					// there may be more than one remaining variable that splits a rho, create a tree
					int minSplitSize = Integer.MAX_VALUE;
					ArrayList<String> result = new ArrayList<>();
					for (String possibleVar : entry.getValue()) {
						ArrayList<String> possibleSplitVars = new ArrayList<>();
						possibleSplitVars.addAll(splitVars);
						possibleSplitVars.add(possibleVar);
						ArrayList<String> splitResult = getSplitVars(rhoCoverage, possibleSplitVars, possibleVar);
						if (splitResult.size() < minSplitSize) {
							result = new ArrayList<>();
							result.addAll(splitResult);
							minSplitSize = result.size();
							//System.out.println("Upgraded my solution");
						}
					}
					return result;
				}
			}
			// did not find a split given current split factor, look for another path
			splitFactor++;
		}		
		return splitVars;
	}
	
	// method that prints the splitting parameters for a given vertex
	private void printSplits(int counter, ArrayList<String> splitVars) {
		System.out.println("Split parameters for vertex no" + counter + " -> " + splitVars); 
	}
	
	// method that obtains ranges of a given variable for a given rho
	private Pair<Integer, Integer> getSplitRange(LinkedHashMap<VertexRho, VertexPhi> rhos, String splitVar) {
 		for (Map.Entry<VertexRho, VertexPhi> entry : rhos.entrySet()) {
			if (entry.getKey().getVariables().contains(splitVar)) {
				return entry.getValue().getPhi().get(splitVar);
			}
		}
 		return null;
	}

	// method that applies a given split to a given vertex
	private void applySplit(VertexSigma sigma, ArrayList<String> splits, int txProfile) {
		// generate all the new sigmas for splits
		ArrayList<VertexSigma> newSigmas = generateSigmas(sigma, splits, txProfile);
		// go through sigmas and create new vertices
		for (VertexSigma newSigma : newSigmas) {
			// associate new sigma to a new vertex
			GraphVertex gv = new GraphVertex(newSigma, txProfile, true);
			// other sub vertices need to be disjoint from previously existing ones
			addVertex(gv, splitGraph);
		}
	}
	
	// method that receives split parameters and computes all sigma combinations
	private ArrayList<VertexSigma> generateSigmas(VertexSigma sigma, ArrayList<String> splits, int txProfile) {
		// calculate how many splits per parameter
		int noSplits = 1;
		LinkedHashMap<String, Integer> splitsPerParameter = new LinkedHashMap<>();
		for (String split : splits) {
			// check if table split
			if (split.startsWith("#")) {
				noSplits *= tableSplitFactor;
				splitsPerParameter.put(split, tableSplitFactor);
			}
			// input split case
			else {
				// check the range of this split variable
				Pair<Integer, Integer> inputRange = getSplitRange(sigma.getRhos(), split);
				// split range for this given parameter
				int splitRange = (inputRange.getValue() - inputRange.getKey()) + 1;
				// take into account input split factor
				int splitFactor = inputSplitFactor;
				while (((splitRange / splitFactor) < 1)) {
					// split factor needs to be reduced to match range
					splitFactor--;
				}
				// split factor found, update no splits
				noSplits *= splitFactor;
				System.out.println("Split factor for param: " + split + ": " + splitFactor);
				splitsPerParameter.put(split, splitFactor);
			}
		}
		// knowing split parameters and no parts provided by each param we can generate sigmas
		List<List<VertexSigma>> finalSigmas = new LinkedList<>();
		List<VertexSigma> newSigmas = new LinkedList<>();
		for (int i = 0; i < noSplits; i++) {
			VertexSigma newSigma = new VertexSigma(sigma);
			newSigmas.add(newSigma);
		}
		// initial set of sigmas before splitting is ready, all vertices equal
		finalSigmas.add(newSigmas);
		// apply splits
		for (Map.Entry<String, Integer> split: splitsPerParameter.entrySet()) {
			// how many partitions does this parameter generate
			int noSplitsParameter = split.getValue();
			// break arrays in final sigma into noSplitsPerParameter
			List<List<VertexSigma>> temp = new LinkedList<>();
			for (List<VertexSigma> partitions : finalSigmas) {
				temp.addAll(partition(partitions, noSplitsParameter));
			}
			// update rhos in sublists
			for (int i = 0; i < temp.size(); i++) {
				// update all sigmas in each section, i represents the section
				for (int j = 0; j < temp.get(i).size(); j++) {
					// check if table split or input split
					System.out.println(i % noSplitsParameter);

					if (split.getKey().startsWith("#")) {
						// input split case
						VertexSigma newSigma = tableSplit(temp.get(i).get(j), split.getKey(), noSplitsParameter, i % noSplitsParameter);
						temp.get(i).set(j, newSigma);
					}
					else {
						VertexSigma newSigma = rhoInputSplit(temp.get(i).get(j), split.getKey(), 
								getSplitRange(sigma.getRhos(), split.getKey()), noSplitsParameter, i % noSplitsParameter);
						// update sigma
						temp.get(i).set(j, newSigma);
					}
				}
			}
			// clear old final sigmas not updated
			finalSigmas.clear();
			// update final sigmas with temp
			finalSigmas.addAll(temp);
		}
		// at this point each sublist in finalSigmas has a single element
		ArrayList<VertexSigma> sigmas = new ArrayList<>();
		// build return list
		for (List<VertexSigma> entry : finalSigmas) {
			// these lists have 1 element
			sigmas.addAll(entry);
		}
		System.out.println("Splitting successful");
		return sigmas;
	}
	
	// method that breaks a list into n sublists
	public List<List<VertexSigma>> partition(List<VertexSigma> originalList,int noParts){
		// no elements per sublist
		int partitionSize = originalList.size() / noParts;
		List<List<VertexSigma>> partitions = new LinkedList<List<VertexSigma>>();
		for (int i = 0; i < originalList.size(); i += partitionSize) {
		    partitions.add(originalList.subList(i,
		            Math.min(i + partitionSize, originalList.size())));
		}
		return partitions;
	}
	
	// method that applies rho input split by manipulating phi ranges
	private VertexSigma rhoInputSplit(VertexSigma sigma, String splitParam, Pair<Integer, Integer> inputRange, int noSplits, int section) {
		// calculate cutoff for new vertex phis
		int cutoff = (inputRange.getValue() - inputRange.getKey() + 1) / noSplits;
		// update all phis in the vertex
		for (Map.Entry<VertexRho, VertexPhi> rhoPhi: sigma.getRhos().entrySet()) {
			// if the rho contains the split param its respective phi needs to be updated
			if (rhoPhi.getKey().getVariables().contains(splitParam)) {
				if (section == 0) rhoPhi.getValue().getPhi().put(splitParam, 
						new Pair<Integer, Integer>(inputRange.getKey(), cutoff - 1));
				else 
					rhoPhi.getValue().getPhi().put(splitParam, 
							new Pair<Integer, Integer>(cutoff * section, (cutoff * (section) + 1) - 1));
			}
		}
		return sigma;
	}
	
	// method that applies table split based on rho output limitation
	private VertexSigma tableSplit(VertexSigma sigma, String splitParam, int noSplits, int section) {
		// obtain table number by trimming meta char
		String tableNo = splitParam.substring(1);
		// check the range of items in this table
		int tableRange = VertexPhi.getTableRange(Integer.parseInt(tableNo));
		// calculate cutoff for section
		int cutoff = (tableRange / noSplits);
		// update all phis in the vertex
		for (Map.Entry<VertexRho, VertexPhi> rhoPhi: sigma.getRhos().entrySet()) {
			// update every access to the table under split
			if (rhoPhi.getKey().getRho().startsWith(tableNo)) {
				if (section == 0) 
					rhoPhi.getKey().splitRho(" <= " + (cutoff));
				else {
					rhoPhi.getKey().splitRho(" > " + (cutoff * section));
					rhoPhi.getKey().splitRho(" <= " + (cutoff * (section + 1)));				
				}
			}
		}
		return sigma;
	}
	
	// method that adds a vertex to the graph, ensuring no vertex overlaps
	private void addVertex(GraphVertex newVertex, LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph) {
		// edges found during rho comparison
		ArrayList<GraphEdge> foundEdges = new ArrayList<>();
	
		// need to compare newly added vertex to every other vertex
		HashMap<VertexRho, VertexPhi> rhosV = newVertex.getSigma().getRhos();
		
		for (Map.Entry<VertexRho, VertexPhi> entryV: rhosV.entrySet()) {
			String rhoV = entryV.getKey().getRho();
			String phiV = entryV.getValue().getPhiAsString();
			
			if (entryV.getKey().isRemote()) continue;
			
			for (Map.Entry<GraphVertex, ArrayList<GraphEdge>> node : graph.entrySet()) {
				GraphVertex gv = node.getKey();
				// obtain all rhos in previously existing vertex
				HashMap<VertexRho, VertexPhi> rhosGV = gv.getSigma().getRhos();
				for (Map.Entry<VertexRho, VertexPhi> entryGV: rhosGV.entrySet()) {
					String rhoGV = entryGV.getKey().getRho();
					String phiGV = entryGV.getValue().getPhiAsString();
					// if rhos are not on same table they do need to be compared
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
		graph.put(newVertex, new ArrayList<>());
		
		// add its edges to the graph as well
		for (GraphEdge e : foundEdges) {
			addEdge(e, graph);
		}
		// compute vertex weight
		newVertex.computeVertexWeight();	
		
		System.out.println("Subvertex added successfully");
	}
	
	// method that prepares phi for mathematica query, appending any update if needed
	private String preparePhi(String phi, String update) {
		String preparedPhi = new String(phi);
		if (update != null) {
			// if there is a constraint on rho, phi needs an update
			 preparedPhi = "(" + phi +  " && (" + update + "))";
		}
		return preparedPhi;
	}
	
	// method that computers rho intersection and outputs their symbolic intersection
	private String rhoIntersection(String rho1, String rho2, String phi1, String phi2, HashSet<String> vars1, HashSet<String> vars2) {
		KernelLink link = MathematicaHandler.getInstance();
		
		rho2 = variableRename(rho2);
		phi2 = variableRename(phi2);
		
		String rhoQuery = rho1.substring(rho1.indexOf(">") + 1) + " == " + rho2.substring(rho2.indexOf(">") + 1);
		String phiQuery = "(" + phi1 + ") && (" + phi2 + ")";		
				
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
		
		// build mathematica query with simplifier
		String query = "Reduce[" + "Simplify[(" + rhoQuery + ") && ("  + phiQuery + ")]" + ", " 
					+ variables + ", Integers, Backsubstitution -> True]";	
			
		
		String result = link.evaluateToOutputForm(query, 0);
		
		// debug cases, exceptions
		if (result.equals("$Failed"))
			System.out.println("Failed rho intersection query ->" + query);
		if (result.contains("C[1]")) {
			// Mathematica is not working in this case
			System.out.println("Slow intersection due to negative numbers");
			System.out.println(query);
			return "False";
		}
		
		return result;
	}

	// method used to add an edge to a graph
	private void addEdge(GraphEdge edge, LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph) {
		graph.get(edge.getSrc()).add(edge);
	}
	
	// method used to build adjacency matrix for graph presenting
	private LinkedHashMap<Pair<Integer, Integer>, HashMap<Integer, Integer>> buildMETISGraph() {
		// final graph for METIS
		LinkedHashMap<Pair<Integer, Integer>, HashMap<Integer, Integer>> METISGraph = new LinkedHashMap<>();
		// iterate through every vertex
		for (Map.Entry<GraphVertex, ArrayList<GraphEdge>> vertex : splitGraph.entrySet()) {
			// map each edge destiny with a weight
			HashMap<Integer, Integer> edges = new HashMap<>();
			// go through all the edges linked to a vertex
			for (GraphEdge e : vertex.getValue()) {
				// get edge destiny
				int edgeDst = e.getDest().getVertexID();
				// get edge weight
				int edgeWeight = e.getEdgeWeight();
				// add edge to the metis graph
				if (edges.containsKey(edgeDst)) 
					edges.put(edgeDst, edges.get(edgeDst) + edgeWeight);
				else {
					// increment edge counter
					this.noEdges++;
					edges.put(edgeDst, edgeWeight);
				}
			}
			// increment number of vertices
			this.noVertices++;
			METISGraph.put(new Pair<Integer, Integer>(vertex.getKey().getVertexID(), vertex.getKey().getVertexWeight()), edges);
		}
		// edges are bidirectional
		this.noEdges = this.noEdges / 2;
		return METISGraph;
	}
	
	// method to print adjacency matrix given graph
	private void printMatrix(LinkedHashMap<Pair<Integer, Integer>, HashMap<Integer, Integer>> graph) {
		// number of vertices in graph
		int noVertices = graph.size();
		// iterate through each vertex
		for (Map.Entry<Pair<Integer, Integer>, HashMap<Integer, Integer>> vertex : graph.entrySet()) {
			int i = 1;
			while (i <= noVertices) {
				int entry = 0;
				if (graph.get(vertex.getKey()).containsKey(i)) entry = graph.get(vertex.getKey()).get(i);
				if (i != 1) System.out.print(", " + entry);
				else System.out.print(entry);
				i++;
			}
			System.out.print("\n");
		}
	}
	
	// method to print input file for METIS given a graph
	private void printMETISfile(LinkedHashMap<Pair<Integer, Integer>, HashMap<Integer, Integer>> graph) {
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
			metisFile.append(this.noVertices + " " +this.noEdges + " " + "011\n");
			// iterate through all the vertices in the graph
			for (Map.Entry<Pair<Integer, Integer>, HashMap<Integer, Integer>> vertex : graph.entrySet()) {
				// get vertex weight
				int vertexWeight = vertex.getKey().getValue();
				// print vertex line
				metisFile.append(String.valueOf(vertexWeight));
				for (Map.Entry<Integer, Integer> edge : vertex.getValue().entrySet()) {
					// get edge destination
					int edgeDest = edge.getKey();
					// get edge weight
					int edgeWeight = edge.getValue();
					// print edge 
					metisFile.append(" " + edgeDest + " " + edgeWeight);
				}
				// print new line and go to next vertex
				metisFile.append("\n");
			}
			// close file
			metisFile.close();
			System.out.println("Successfully generated METIS file");
		} catch (IOException e) {
			System.out.println("Error on generating METIS file!");
			e.printStackTrace();
		}
	}
	
	// method to check if there is a variable that can split multiple overlapping rhos on same table
	private HashSet<String> checkCommonSplit(ArrayList<Pair<VertexRho, VertexPhi>> rhos) {
		// possible splits
		HashSet<String> splits = new HashSet<>();
		// set of common variables in every rho on the same table
		HashSet<String> commonVars = new HashSet<>();
		// check commonVars accross all rhos
		boolean isFirst = true;
		for (Pair<VertexRho, VertexPhi> rho: rhos) {
			if (isFirst) {
				commonVars.addAll(rho.getKey().getVariables());
				isFirst = false;
			}
			else if (commonVars.size() == 0) {
				// no possible commonVars
				break;
			}
			else
				commonVars.retainAll(rho.getKey().getVariables());
		}
		// if commonVars is not empty then we have candidate splits
		for (String commonVar : commonVars) {	
			// intersection flag
			boolean overlap = false;
			// simulate split by commonVar and check overlaps
			for (int i = 0; i < rhos.size(); i++) {
				// get rho
				Pair<VertexRho, VertexPhi> rho1 = rhos.get(i);
				// get range of commonVar
				Pair<Integer, Integer> varRange = rho1.getValue().getPhi().get(commonVar);
				// calculate cutoff for new simulated phi
				int cutoff = (varRange.getValue() - varRange.getKey() + 1) / 2;
				VertexRho rhoCopy1 = new VertexRho(rho1.getKey());
				VertexPhi phiCopy1 = new VertexPhi(rho1.getValue());
				// edit phi for sim 
				phiCopy1.getPhi().put(commonVar, new Pair<Integer, Integer>(varRange.getKey(), cutoff));
				// build rho phi pair
				Pair<VertexRho, VertexPhi> rhoPhi1 = new Pair<VertexRho, VertexPhi>(rhoCopy1, phiCopy1);
				for (int j = 0; j < rhos.size(); j++) {
					// get rho
					Pair<VertexRho, VertexPhi> rho2 = rhos.get(j);
					VertexRho rhoCopy2 = new VertexRho(rho2.getKey());
					VertexPhi phiCopy2 = new VertexPhi(rho2.getValue());
					// edit phi for sim 
					phiCopy2.getPhi().put(commonVar, new Pair<Integer, Integer> (cutoff + 1, varRange.getValue()));
					// build rho phi pair
					Pair<VertexRho, VertexPhi> rhoPhi2 = new Pair<VertexRho, VertexPhi>(rhoCopy2, phiCopy2);
					//check inersection 
					overlap = checkIntersection(rhoPhi1, rhoPhi2);
					// if overlapping bin variable
					if (overlap) {
						break;
					}
				}
				// collision detected, common var not good splitter
				if (overlap) {
					break;
				}	
			}
			// no collision detected
			if (!overlap)
				splits.add(commonVar);
		}
		return splits;
	}
}
	
	