package thesis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import com.wolfram.jlink.KernelLink;

import javafx.util.Pair;

public class Splitter {
	
	private LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> splitGraph = new LinkedHashMap<>();
	
	public Splitter() {
		
	}
	
	public LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> splitGraph(LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph) {
		// for each vertex, iterate through its rhos
		int counter = 1;
		// tx profile identifier
		int txProfile = 1;
		for (GraphVertex v: graph.keySet()) {
			// variable that stores rho intersection information
			boolean stop = false;
			// obtain rho phi pair for the given vertex
			HashMap<VertexRho, VertexPhi> rhos = v.getSigma().getRhos();
			// store no of rhos in V
			int noRhos = rhos.size();
			// possible split variables per table
			HashMap<Integer, ArrayList<HashSet<String>>> possibleSplits = new HashMap<>();
			// group together rhos that belong to the same table within a vertex
			HashMap<Integer, ArrayList<Pair<VertexRho, VertexPhi>>> buckets = groupRhos(rhos);
			// after having rhos grouped by table, analyze each table
			for (Map.Entry<Integer, ArrayList<Pair<VertexRho, VertexPhi>>> entry: buckets.entrySet()) {	
				// reset intersection flag
				stop = false;
				// check how many rhos for given table
				if (entry.getValue().size() == 1) {
					// only rho for table, must be split using one of its variables
					ArrayList<HashSet<String>> splits = new ArrayList<>();
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
							ArrayList<HashSet<String>> splits = new ArrayList<>();
							splits.add(rhoPhi1.getKey().getVariables());
							splits.add(rhoPhi2.getKey().getVariables());
							possibleSplits.put(entry.getKey(), splits);
						}
						else {
							// rhos overlap for some input, splitting the table instead
							possibleSplits.remove(entry.getKey());
							// mark possible splits for table as null to know it is a table split
							possibleSplits.put(entry.getKey(), null);
							// set stop variable, indicating table is dealt with
							stop = true;
							break;				
						}
					}
				}
			}		
			// variable that stores table splits and variable splits
			ArrayList<String> splits = new ArrayList<>();
			
			// map between rhoID and variables that cover it
			HashMap<Integer, HashSet<String>> rhoCoverage = new HashMap<>();
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
			ArrayList<String> splitVars = new ArrayList<>();
			// get the smallest hitting set, NP complete problem
			splits.addAll(getSplitVars(rhoCoverage, splitVars, null));
			// display the splits
			printSplits(counter, splits);
			// apply the split to the vertex
			applySplit(v.getSigma(), splits, txProfile);
			
			//increment vertex counter
			counter++;
		}
		return splitGraph;
	}
	
	private HashMap<Integer, ArrayList<Pair<VertexRho, VertexPhi>>> groupRhos(HashMap<VertexRho, VertexPhi> rhos) {
		HashMap<Integer, ArrayList<Pair<VertexRho, VertexPhi>>> buckets = new HashMap<>();
		for (Map.Entry<VertexRho, VertexPhi> entry : rhos.entrySet()) {
			Pair<VertexRho, VertexPhi> rhoPhi = new Pair<VertexRho, VertexPhi>(entry.getKey(), entry.getValue());
			Integer tableNo = Integer.parseInt(rhoPhi.getKey().getRho().substring(0, rhoPhi.getKey().getRho().indexOf(">") - 1));
			if (buckets.containsKey(tableNo)) {
				buckets.get(tableNo).add(rhoPhi);
			}
			else {
				ArrayList<Pair<VertexRho, VertexPhi>> bucket = new ArrayList<>();
				bucket.add(rhoPhi);
				buckets.put(tableNo, bucket);
			}
		}
		return buckets;
	}
	
	private boolean checkIntersection(Pair<VertexRho, VertexPhi> rhoPhi1, Pair<VertexRho, VertexPhi> rhoPhi2) {
		String rho1 = rhoPhi1.getKey().getRho();
		String rho2 = rhoPhi2.getKey().getRho();
		String phi1 = rhoPhi1.getValue().getPhiAsString();
		String phi2 = rhoPhi2.getValue().getPhiAsString();
		
		if (rhoPhi1.getKey().getRhoUpdate() != null) {
			phi1 += " && !(" + rhoPhi1.getKey().getRhoUpdate()+ ")"; 
		}
		if (rhoPhi2.getKey().getRhoUpdate() != null) {
			phi2 += " && !(" + rhoPhi2.getKey().getRhoUpdate()+ ")"; 
		}
	
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
	
	private String variableRename(String s) {
		// rename variables to prepare mathematica query
		return s.replaceAll("id", "idGV");
	}
	
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
	
	private Pair<Integer, Integer> getSplitRange(LinkedHashMap<VertexRho, VertexPhi> rhos, String splitVar) {
 		for (Map.Entry<VertexRho, VertexPhi> entry : rhos.entrySet()) {
			if (entry.getKey().getVariables().contains(splitVar)) {
				System.out.println("Split var: " + splitVar + " | Range -> " + entry.getValue().getPhi().get(splitVar));
				return entry.getValue().getPhi().get(splitVar);
			}
		}
 		return null;
	}

	private void printSplits(int counter, ArrayList<String> splitVars) {
		System.out.println("Var list for vertex no" + counter + " -> " + splitVars); 
	}
	
	private void applySplit(VertexSigma sigma, ArrayList<String> splits, int txProfile) {
		// give an ID to each vertex consisting of a byte array
		ArrayList<String> ids = generateCombinations(splits.size(), new int[splits.size()], 0);
		// flag to note if dealing with first subvertex
		boolean isFirst = true;
		// iterate over all split combinations
		for (String id: ids) {
			// create a new sub vertex sigma
			VertexSigma newSigma = new VertexSigma(sigma);
			// each ith element of the string is mapped to the ith parameter in splitVars
			for (int i = 0; i < id.length(); i++) {
				String splitParam = splits.get(i);
				char paramValue = id.charAt(i);		
				if (splitParam.startsWith("#")) {
					// table split, check whether this param is a "lower" or "upper" split
					if (paramValue == '0') {
						// lower table split
						newSigma = tableSplit(newSigma, splitParam, 0);
					}
					else {
						//upper table split
						newSigma = tableSplit(newSigma, splitParam, 1);
					}
				}
				else {
					// input split, check whether this param is a "lower" or "upper" split, get range
					Pair<Integer, Integer> inputRange = getSplitRange(sigma.getRhos(), splitParam);
					if (paramValue == '0') {
						// lower input split
						newSigma = rhoInputSplit(newSigma, splitParam, inputRange, 0);
					}
					else {
						//upper input split
						newSigma = rhoInputSplit(newSigma, splitParam, inputRange, 1);
					}
				}
			}
			// associate new sigma to a new vertex
			GraphVertex gv = new GraphVertex(newSigma, txProfile);
			
			// add vertex to graph
			if (isFirst) {
				// first subvertex does not need logical subtraction
				isFirst = false;
				splitGraph.put(gv, new ArrayList<>());
				gv.computeVertexWeight();
			}
			else {
				// other sub vertices need to be disjoint from previously existing ones
				addVertex(gv, txProfile, splitGraph);
				gv.printVertex();
			}
		}
		
	}
	
	private ArrayList<String> generateCombinations(int n, int comb[], int pos) {
		ArrayList<String> ids = new ArrayList<>();
		
		if (pos == n) {
			int temp = 0;
			for (int i = n - 1; i >= 0; i--) { 
				temp = (int)(Math.pow(10, i)*comb[i] + temp);
			}
			ids.add(String.format("%0"+ n +"d", temp));
			return ids;
		}
		comb[pos] = 0; 
	    ids.addAll(generateCombinations(n, comb, pos + 1));
	    comb[pos] = 1; 
	    ids.addAll(generateCombinations(n, comb, pos + 1));
	    return ids;
	}

	private VertexSigma rhoInputSplit(VertexSigma sigma, String splitParam, Pair<Integer, Integer> inputRange, int section) {
		// calculate cutoff for new vertex phis
		int cutoff = (inputRange.getValue() - inputRange.getKey() + 1) / 2;
		// update all phis in the vertex
		for (Map.Entry<VertexRho, VertexPhi> rhoPhi: sigma.getRhos().entrySet()) {
			// if the rho contains the split param its respective phi needs to be updated
			if (rhoPhi.getKey().getVariables().contains(splitParam)) {
				if (section == 0) rhoPhi.getValue().getPhi().put(splitParam, 
						new Pair<Integer, Integer>(inputRange.getKey(), cutoff));
				else 
					rhoPhi.getValue().getPhi().put(splitParam, 
							new Pair<Integer, Integer>(cutoff+1, inputRange.getValue()));
			}
		}
		return sigma;
	}

	private VertexSigma tableSplit(VertexSigma sigma, String splitParam, int section) {
		// obtain table number by trimming meta char
		String tableNo = splitParam.substring(1);
		// check the range of items in this table
		int tableRange = VertexPhi.getTableRange(Integer.parseInt(tableNo));
		// update all phis in the vertex
		for (Map.Entry<VertexRho, VertexPhi> rhoPhi: sigma.getRhos().entrySet()) {
			// update every access to the table under split
			if (rhoPhi.getKey().getRho().startsWith(tableNo)) {
				if (section == 0) 
					rhoPhi.getKey().splitRho(" <= " + tableRange / 2);
				else
					rhoPhi.getKey().splitRho(" > " + tableRange / 2);
			}
		}
		return sigma;
	}
	
	private void addVertex(GraphVertex newVertex, int txProfile, LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph) {
		// edges found during rho comparison
		ArrayList<GraphEdge> foundEdges = new ArrayList<>();
	
		// need to compare newly added vertex to every other vertex
		HashMap<VertexRho, VertexPhi> rhosV = newVertex.getSigma().getRhos();
		for (Map.Entry<GraphVertex, ArrayList<GraphEdge>> node : graph.entrySet()) {
			GraphVertex gv = node.getKey();
			// obtain all rhos in previously existing vertex
			HashMap<VertexRho, VertexPhi> rhosGV = gv.getSigma().getRhos();
			// iterate through the new rhos being added
			for (Map.Entry<VertexRho, VertexPhi> entryV: rhosV.entrySet()) {
				// don't compare remote rhos as they are elsewhere
				if (entryV.getKey().isRemote()) {
					ArrayList<GraphEdge> remoteEdges = redrawEdges(newVertex, entryV.getKey(), entryV.getValue(), txProfile, graph);
					// add all remote edges found
					for (GraphEdge e : remoteEdges) {
						foundEdges.add(e);
					}
					continue;
				}
				
				String rhoV = entryV.getKey().getRho();
				String phiV = entryV.getValue().getPhiAsString();
				
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
		graph.put(newVertex, new ArrayList<>());
		
		// add its edges to the graph as well
		for (GraphEdge e : foundEdges) {
			addEdge(e, graph);
		}
		// compute vertex weight
		newVertex.computeVertexWeight();	
		
		System.out.println("Subvertex added successfully");
	}
	
	private String preparePhi(String phi, String update) {
		String preparedPhi = new String(phi);
		if (update != null) {
			// if there is a constraint on rho, phi needs an update
			 preparedPhi = "(" + phi +  " && (" + update + "))";
		}
		return preparedPhi;
	}
	
	private String rhoIntersection(String rho1, String rho2, String phi1, String phi2, HashSet<String> vars1, HashSet<String> vars2) {
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
		
		if (result.equals("$Failed"))
			System.out.println("Failed rho intersection query ->" + query);
		if (result.contains("C[1]")) {
			// Mathematica is not working in this case
			System.out.println("Slow intersection due to negative numbers");
			System.out.println(query);
			return "False";
		}
		if (result.contains("Integers") && result.contains("oliid")) {
			System.out.println(rho1);
			System.out.println(rho2);
			System.out.println(phi1);
			System.out.println(phi2);
			System.out.println(query);
		}
		
		return result;
	}

	private void addEdge(GraphEdge edge, LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph) {
		graph.get(edge.getSrc()).add(edge);
	}

	private ArrayList<GraphEdge> redrawEdges(GraphVertex newVertex, VertexRho remoteRho, VertexPhi remotePhi, int txProfile, LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph) {
		// list of edges found based on remote rho
		ArrayList<GraphEdge> remoteEdges = new ArrayList<>();
		for (Map.Entry<GraphVertex, ArrayList<GraphEdge>> entry: graph.entrySet()) {
			// compare current vertex with all previously existing ones to settle remote edges
			if (txProfile > entry.getKey().getTxProfile()) {
				// check intersection between remoteRho and its pair
				String remRho = remoteRho.getRho();
				String remPhi = remotePhi.getPhiAsString();
				// check every rho
				for (Map.Entry<VertexRho, VertexPhi> entryGV: entry.getKey().getSigma().getRhos().entrySet()) {
					String rhoGV = entryGV.getKey().getRho();
					String phiGV = entryGV.getValue().getPhiAsString();
					// if rhos are not on same table they do need to be compared
					if (!remRho.substring(0, remRho.indexOf(">") - 1).equals(rhoGV.substring(0, rhoGV.indexOf(">") - 1))
							|| entryGV.getKey().isRemote())
						continue;
					String result = null;
					String remPhiQ = preparePhi(remPhi, remoteRho.getRhoUpdate());
					String phiGVQ = preparePhi(phiGV, entryGV.getKey().getRhoUpdate());
					result = rhoIntersection(remRho, rhoGV, remPhiQ, phiGVQ, remoteRho.getVariables(), entryGV.getKey().getVariables());
					// check the intersection results
					if (result.equals("False")) {
						// no overlap so no edge redrawing
						continue;
					}
					else {
						System.out.println("Collision found, redrawing remote edge");
						// collision found, adding remote edge to list
						GraphEdge remEdge = new GraphEdge(newVertex, entry.getKey(), remRho, rhoGV, result, remotePhi.getPhiAsGroup());
						remoteEdges.add(remEdge);
					}
				}
			}
		}
		
		return remoteEdges;
	}
}
	
	