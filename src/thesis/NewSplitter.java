package thesis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

// class performing vertex splitting and creating graph file for the partitioner
public class NewSplitter {	
	// counter for SMT file creation
	int fileId = 1;
	// count for SMT tags
	int smtTag = 0;
	// replication wanted?
	boolean replication = true;
	// how many parts does a table split go for
	public int tableSplitFactor = 2;
	// how many splits does an input split go for, at most
	public int inputSplitFactor = GraphBuilder.noP;
	// no edges in final graph
	int noEdges = 0;
	// no vertices in final graph
	int noVertices = 0;
	// graph resultant from splitting
	private LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> splitGraph = new LinkedHashMap<>();
	// splits saved for each vertex
	private LinkedHashMap<Integer, ArrayList<String>> splits = new LinkedHashMap<>();
	// set of previous splits used
	private HashSet<String> previousSplits = new HashSet<>();
	
	// method that takes a graph as input and splits its vertices, returns the resulting graph
	public LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> splitGraph(LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph) {
		
		long timeForSplitting = 0;
		long timeForComputation = 0;
		
		// tx profile identifier
		int txProfile = 1;
		// go through all vertices in graph
		for (GraphVertex v: graph.keySet()) {
			long startTime = System.currentTimeMillis();
			// obtain rho phi pair for the given vertex
			HashMap<VertexRho, VertexPhi> rhos = v.getSigma().getRhos();
			// group together rhos that belong to the same table within a vertex
			HashMap<Integer, ArrayList<Pair<VertexRho, VertexPhi>>> buckets = groupRhos(rhos);
			System.out.println("Group rhos, no buckets " + buckets.size() + " - DONE");
			// after having rhos grouped by table, analyze each table and get possible splits
			HashMap<Integer, ArrayList<HashSet<String>>> possibleSplits = analyseTables(buckets);
			
			System.out.println("Analyze Tables - DONE");
			// get the shortest splitting variable set and table splits needed
			ArrayList<String> splits = getSplits(possibleSplits);
			System.out.println("Getting splits - DONE");
			// display the splits
			printSplits(txProfile, splits);
			long endTime = System.currentTimeMillis();
			long splitTime = endTime - startTime;
			timeForSplitting += splitTime;
			// apply the split to the vertex
			startTime = System.currentTimeMillis();
			applySplit(v.getSigma(), splits, txProfile);
			endTime = System.currentTimeMillis();
			long computationTime = endTime - startTime;
			timeForComputation += computationTime;
			//increment vertex counter
			txProfile++;
		}
		System.out.println("Time to split the graph: " + timeForSplitting);
		System.out.println("Time to compute weights of vertices and edges: " + timeForComputation);
		// build metis graph
		LinkedHashMap<Integer, Pair<Integer, HashMap<Integer, Integer>>> metisGraph = buildMETISGraph();
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
			// ignore extra indexes TPC C
			if (tableNo > 9) continue;
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
				HashSet<String> possibleVars = entry.getValue().iterator().next().getKey().getVariables();
				HashSet<String> verifiedVars = new HashSet<>();
				// check the range of each of the variables
				for (String possibleVar: possibleVars) {
					Pair<Integer, Integer> varRange = entry.getValue().iterator().next().getValue().getPhi().get(possibleVar);
					if (varRange.getValue()-varRange.getKey() > 0 && !possibleVar.startsWith("ir")) {
						// in this case this var can be a splitter
						verifiedVars.add(possibleVar);
					}
				}
				if (!verifiedVars.isEmpty()) {
					splits.add(verifiedVars);
					possibleSplits.put(entry.getKey(), splits);
					continue;
				}
				else {
					// check if replication is wanted
					if (replication) {
						// check whether this table is possible to replicate
						if (VertexPhi.checkTableReplicated(entry.getKey())) {
							// do not consider this table
							possibleSplits.remove(entry.getKey());
							continue;
						}
						else {
							// rhos overlap for some input, splitting the table instead
							possibleSplits.remove(entry.getKey());
							// mark possible splits for table as null to know it is a table split
							possibleSplits.put(entry.getKey(), null);
							continue;
						}
					}
					else {
						// table does not have any valid split vars, proceed with table split
						possibleSplits.remove(entry.getKey());
						// mark possible splits for table as null to know it is a table split
						possibleSplits.put(entry.getKey(), null);
						continue;
					}
				}
				
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
						HashSet<String> possibleVars = rhoPhi1.getKey().getVariables();
						HashSet<String> verifiedVars = new HashSet<>();
						for (String possibleVar: possibleVars) {
							Pair<Integer, Integer> varRange = rhoPhi1.getValue().getPhi().get(possibleVar);
							if (varRange.getValue() - varRange.getKey() > 0 && !possibleVar.startsWith("ir")) 
								verifiedVars.add(possibleVar);
						}						
						possibleVars = rhoPhi2.getKey().getVariables();
						for (String possibleVar: possibleVars) {
							Pair<Integer, Integer> varRange = rhoPhi2.getValue().getPhi().get(possibleVar);
							if (varRange.getValue() - varRange.getKey() > 0 && !possibleVar.startsWith("ir")) 
								verifiedVars.add(possibleVar);
						}						
						if (!verifiedVars.isEmpty()) {
							splits.add(verifiedVars);
							possibleSplits.put(entry.getKey(), splits);
							continue;
						}
					}
					else {				
						// check if there is a split variable common amongst all rhos in the table under analysis
						HashSet<String> commonSplit = checkCommonSplit(entry.getValue());
						// check if any vars found
						System.out.println(entry.getKey() + " Common split size:" + commonSplit.size());
						if (commonSplit.size() != 0) {							
							// split found, add it
							splits.add(commonSplit);
							possibleSplits.put(entry.getKey(), splits);
						}
						else {
							// check if replication is wanted
							if (replication) {	
								// check whether this table is possible to replicate
								if (VertexPhi.checkTableReplicated(entry.getKey())) {
									// do not consider this table
									possibleSplits.remove(entry.getKey());
								}
								else {
									// rhos overlap for some input, splitting the table instead
									possibleSplits.remove(entry.getKey());
									// mark possible splits for table as null to know it is a table split
									possibleSplits.put(entry.getKey(), null);
								}
							}
							else {
								// rhos overlap for some input, splitting the table instead
								possibleSplits.remove(entry.getKey());
								// mark possible splits for table as null to know it is a table split
								possibleSplits.put(entry.getKey(), null);	
							}
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
	
	// method that checks sat between intersection of 2 rhos and corresponding phis
	private boolean checkIntersection(Pair<VertexRho, VertexPhi> rhoPhi1, Pair<VertexRho, VertexPhi> rhoPhi2) {
		// rhos 
		VertexRho rhoV = rhoPhi1.getKey();
		VertexRho rhoGV = rhoPhi2.getKey();
		// phis
		VertexPhi phiV = rhoPhi1.getValue();
		VertexPhi phiGV = rhoPhi2.getValue();
		
		// obtain strings for query
		String rho1 = rhoV.getRho(); 
		String rho2 = rhoGV.getRho();
		
		rho2 = variableRename(rho2);
		
		String phi1 = phiV.getPhiAsString(); 
		String phi2 = phiGV.getPhiAsString();
		
		phi2 = variableRename(phi2);
		
		HashSet<String> vars1 = rhoV.getVariables(); 
		HashSet<String> vars2 = rhoGV.getVariables(); 

		// create SMT file
		String fileName = "SMTfile.smt2";
		File smtFile = new File(fileName);
		try {
			smtFile.createNewFile();
		} catch (IOException e) {
			System.out.println("Error while creating SMT file!");
			e.printStackTrace();
		}
		// write to SMT file
		try {
			// reset tags for SMT vars in file
			smtTag = 0;
			
			FileWriter smtWritter = new FileWriter(fileName);
			// first line contains smt lib headers
			smtWritter.append("(set-logic QF_LIA)\n");
			smtWritter.append("(set-info :smt-lib-version 2.0)\n");
			smtWritter.append("(declare-fun rho_1 () Int)\n");
			smtWritter.append("(declare-fun rho_2 () Int)\n");
			smtWritter.append("(declare-fun phi_1 () Bool)\n");
			smtWritter.append("(declare-fun phi_2 () Bool)\n");
			
			for (String variable : vars1) {
				smtWritter.append("(declare-fun " + variable + " () Int)\n");
			}
			for (String variable : vars2) {
				variable = variable.replaceAll("id", "idGV");
				smtWritter.append("(declare-fun " + variable + " () Int)\n");
			}
			
			smtWritter.append("(assert (= rho_1 " + recursiveRhoToSMTLIA(rho1.substring(rho1.indexOf(">") + 1), false) + "))\n");
			smtWritter.append("(assert (= rho_2 " + recursiveRhoToSMTLIA(rho2.substring(rho2.indexOf(">") + 1), false) + "))\n");
			smtWritter.append("(assert (= phi_1 " + parsePhiIntoSmtLIA(phi1) + "))\n");
			smtWritter.append("(assert (= phi_2 " + parsePhiIntoSmtLIA(phi2) + "))\n");

			smtWritter.append("(assert (and (= rho_1 rho_2) (and phi_1 phi_2)))\n");
			
			smtWritter.append("(check-sat)\n");
			smtWritter.append("(exit)");
			// close file
			smtWritter.close();
			fileId++;
			
			// SMT file is built, only need to count number of solutions using approxmc and opensmt
			Runtime rt = Runtime.getRuntime();
			String[] commandsSMT = {"./resources/opensmt", "SMTfile.smt2"};
			Process proc = rt.exec(commandsSMT);

			BufferedReader stdInput = new BufferedReader(new 
			     InputStreamReader(proc.getInputStream()));

			BufferedReader stdError = new BufferedReader(new 
			     InputStreamReader(proc.getErrorStream()));
			
			
			// Read the output from the command
			String s = null;
			while ((s = stdInput.readLine()) != null) {
			    if (s.equals("unsat")) {
					return false;
			    }
			}

			
			// Read any errors from the attempted command
			while ((s = stdError.readLine()) != null) {
				System.out.println("Error during openSMT phase - pre splitting");
			    System.out.println(s);
			    System.exit(0);
			}
			
		} catch (IOException e) {
			System.out.println("Error on generating SMT file!");
			e.printStackTrace();
		} 
		
		return true;
	}
	
	// method that coverts rho into smt lib format LIA
	private String recursiveRhoToSMTLIA(String rho, boolean rho1) {
		// find splitting index related to external operation
		int splitIndex = findExternalOperation(rho);
		
		// if splitIndex is -1 then we reached an operand
		if (splitIndex == -1) {
			return findOperand(rho);
		}		
		// find which operation has been found as external
		if (rho.charAt(splitIndex) == '+') {
			return "(+ " + recursiveRhoToSMTLIA(rho.substring(1, splitIndex - 1), rho1) + " " 
					+ recursiveRhoToSMTLIA(rho.substring(splitIndex + 2, rho.length() - 1), rho1) + ")";
		}
		else if (rho.charAt(splitIndex) == '*') {
			return "(* " + recursiveRhoToSMTLIA(rho.substring(1, splitIndex - 1), rho1) + " " 
					+ recursiveRhoToSMTLIA(rho.substring(splitIndex + 2, rho.length() - 1), rho1) + ")";
		}
		else if (rho.charAt(splitIndex) == '-') {
			return "(- " + recursiveRhoToSMTLIA(rho.substring(1, splitIndex - 1), rho1) + " " 
					+ recursiveRhoToSMTLIA(rho.substring(splitIndex + 2, rho.length() - 1), rho1) + ")";
		}
		
		// shouldn't happen
		System.out.println("Buggy conversion from rho to smt rho");
		return null;
	}
	
	// method that converts phis into smt lib format
	private String parsePhiIntoSmtLIA(String phi) {
		
		String[] parts = phi.split("&&");
		
		String smtString = "(and ";
				
		for (String part: parts) {
			String member = "";
			
			String leftLimit = "";
			String variable = "";
			String rightLimit = "";
			boolean variableDone = false;
			
			for (int i = 0; i < part.length(); i++){ 
			    char c = part.charAt(i);
			    
				// left limit building
				if (Character.isDigit(c) && variable == "") {
			    	leftLimit += c;
			    }
				// right limit building
				else if (Character.isDigit(c) && variableDone) {
			    	rightLimit += c;
			    }
				// variable building
				else if (Character.isLetter(c) || (Character.isDigit(c) && !variableDone)) {
					variable += c;
				}
				//operators
				else if (c == '<' || c == '=') {
					continue;
				}
				// end of a member
				else if (c == ' ' || Character.isWhitespace(c)) {
					if (variable != "") {
						variableDone = true;
					}
					continue;
				}
			}
			
			int leftLimitInt = Integer.parseInt(leftLimit);
			int rightLimitInt = Integer.parseInt(rightLimit);

			
			rightLimitInt = rightLimitInt + 1;
			
			member += "(>= " + variable + " " + leftLimitInt + ") (< " + variable + " " + rightLimitInt + ")";  

			smtString += member;
			
			
			
			
		}
		
		// close the and bracket
		smtString += ")";
		
		return smtString;
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
				// table split detected, store it unless 1 key table
				if (VertexPhi.getTableRange(entry.getKey()) == 0) {
					System.out.println("Table split, unique key!");
					continue;
				}
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
		// save splits
		for (String split: splits) {
			this.previousSplits.add(split);
		}
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
		// parameter that caps how many split vars a rho can have 
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
						// pick split that is alligned with previous splits
						if (splitResult.size() == minSplitSize) {
							// boolean previous split alligned
							boolean alligned = false;
							for (String res : result) {
								// if current result is alligned with previous splits nothing is to be done
								if (this.previousSplits.contains(res)) {
									alligned = true;
									break;
								}
							}
							// if current best split is not alligned with previous splits, check if the new equal split is
							if (!alligned) {
								for (String res : splitResult) {
									if (this.previousSplits.contains(res)) {
										result = new ArrayList<>();
										result.addAll(splitResult);
										minSplitSize = result.size();
										break;
									}
								}
							}
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
		splits.put(counter, splitVars);
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
		// check whether we are in a 1w workload for delivery split LIMITATION?
		if (txProfile == 3 && splits.size() != 1) {
			GraphVertex gv = new GraphVertex(sigma, txProfile, true);
			addVertexSMT(gv, splitGraph);
			System.out.println("Applying splits - DONE");
			return;
		}
		
		// generate all the new sigmas for splits
		ArrayList<VertexSigma> newSigmas = generateSigmas(sigma, splits, txProfile);
		// go through sigmas and create new vertices
		for (VertexSigma newSigma : newSigmas) {
			// associate new sigma to a new vertex
			GraphVertex gv = new GraphVertex(newSigma, txProfile, true);
			// adding new vertex to graph
			System.out.println("Adding new subvertex");
			// other sub vertices need to be disjoint from previously existing ones
			addVertexSMT(gv, splitGraph);
		}
		System.out.println("Applying splits - DONE");
	}
	
	// method that receives split parameters and computes all sigma combinations
	private ArrayList<VertexSigma> generateSigmas(VertexSigma sigma, ArrayList<String> splits, int txProfile) {
		// calculate how many splits per parameter
		int noSplits = 1;
		LinkedHashMap<String, Integer> splitsPerParameter = new LinkedHashMap<>();
		for (String split : splits) {
			// check if table split
			if (split.startsWith("#")) {
				// if there is an input split already, simply split table in 2
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
				// split factor found, update number of splits
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
					if (split.getKey().startsWith("#")) {
						// table split case
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
		System.out.println("Splitting successful - DONE");
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
			
			// unless the table has been table split
			boolean beenSplit = false;
			String tableSplit = "#" + rhoPhi.getKey().getTable();
			for (Split s : sigma.getSplits()) {
				if (s.getSplitParam().equals(tableSplit)) {
					beenSplit = true;
				}
			}
			
			if (beenSplit) continue;
			
			
			if (rhoPhi.getKey().getVariables().contains(splitParam)) {
				if (section == 0) rhoPhi.getValue().getPhi().put(splitParam, 
						new Pair<Integer, Integer>(inputRange.getKey(), cutoff - 1));
				else 
					rhoPhi.getValue().getPhi().put(splitParam, 
							new Pair<Integer, Integer>(cutoff * section, (cutoff * (section + 1) - 1)));
			}
		}
		
		if (section == 0) {
			Split split = new Split(splitParam, inputRange.getKey(), cutoff - 1);
			sigma.addSplit(split);
		}
		else {
			Split split = new Split(splitParam, cutoff * section, (cutoff * (section + 1) - 1));
			sigma.addSplit(split);
		}
		
		
		System.out.println("Applied split param: " + splitParam);
		
		return sigma;
	}
	
	// method that applies table split based on rho output limitation
	private VertexSigma tableSplit(VertexSigma sigma, String splitParam, int noSplits, int section) {
		// obtain table number by trimming meta char
		String tableNo = splitParam.substring(1);
		// check the range of items in this table
		int tableRange = VertexPhi.getTableRange(Integer.parseInt(tableNo));
		// calculate cutoff for section
		int cutoff = ((tableRange + 1) / noSplits);
		// update all phis in the vertex
		for (Map.Entry<VertexRho, VertexPhi> rhoPhi: sigma.getRhos().entrySet()) {
			// update every access to the table under split
			if (rhoPhi.getKey().getRho().startsWith(tableNo)) {
				if (section == 0) {
					rhoPhi.getKey().splitRho(0,cutoff);
				}
				else {
					rhoPhi.getKey().splitRho((cutoff * section), (cutoff * (section + 1)));
				}
			}
		}
		
		if (section == 0) {
			Split split = new Split(splitParam, 0, cutoff);
			sigma.addSplit(split);
		}
		else {
			Split split = new Split(splitParam, (cutoff * section), (cutoff * (section + 1)));
			sigma.addSplit(split);
		}
		
		System.out.println("Applied table split!");
		
		return sigma;
		
	}
	
	// method that adds vertex to the graph
	// method that adds a vertex to the graph, ensuring no edge overlaps
	private void addVertexSMT(GraphVertex newVertex, LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph) {
		// edges found during rho comparison
		ArrayList<GraphEdge> foundEdges = new ArrayList<>();
		// need to compare newly added vertex to every other vertex
		LinkedHashMap<VertexRho, VertexPhi> rhosV = newVertex.getSigma().getRhos();
		// compare rho by rho
		for (Map.Entry<VertexRho, VertexPhi> entryV: rhosV.entrySet()) {
			String rhoV = entryV.getKey().getRho();
			// compute weight of the rho, initally before any subtraction
			int rhoVWeight = computeRhoWeight(entryV.getKey(), entryV.getValue());
			// set the rho weight in a field 
			entryV.getKey().setNoItems(rhoVWeight);
			// if replication is enabled and read only table, dont compare
			if (replication && VertexPhi.checkTableReplicated(Integer.parseInt(rhoV.substring(0, rhoV.indexOf(">") - 1)))) {
				// no need to consider item removal;
			}	
			else {
				// compare new vertex to all vertices in the graph to ensure they are disjoint 
				for (Map.Entry<GraphVertex, ArrayList<GraphEdge>> node : graph.entrySet()) {
					GraphVertex gv = node.getKey();
					// obtain all rhos in previously existing vertex
					LinkedHashMap<VertexRho, VertexPhi> rhosGV = gv.getSigma().getRhos();
					for (Map.Entry<VertexRho, VertexPhi> entryGV: rhosGV.entrySet()) {
						// skip low prob rho
						if (entryGV.getKey().getProb() < 0.5) {
							continue;
						}
						String rhoGV = entryGV.getKey().getRho();
						// if rhos are not on same table they do need to be compared
						if (!rhoV.substring(0, rhoV.indexOf(">") - 1).equals(rhoGV.substring(0, rhoGV.indexOf(">") - 1))
								|| entryGV.getKey().isRemote()) {
							continue;
						}
						
						//compute intersection between rhos given the phis, compute weight of intersection
						//System.out.println("Computing rho intersection!");
						int result = rhoIntersection(entryV.getKey(), entryGV.getKey(), entryV.getValue(), entryGV.getValue());
						//System.out.println("RHo intersection done!");
						// check the intersection results
						if (result == 0) {
							// no overlap so no subtraction needed
							continue;
						}
						if (result < 0) {
							System.out.println("Negative rho interesection result");
						}
						else {
							// variable to store edge weight
							int edgeWeight = result;
							// already have the weight of the edge, check if edges outgoing from V overlap with newly found edge
							for (GraphEdge e : foundEdges) {
								int edgeOverlap = 0;
								// if rhos are not on same table they do need to be compared
								if (!rhoV.substring(0, rhoV.indexOf(">") - 1).equals(e.getEdgeRho().getRho().substring(0, 
										e.getEdgeRho().getRho().indexOf(">") - 1))) {
									continue;
								}
								else {
									edgeOverlap = rhoIntersection(entryV.getKey(), e.getEdgeRho(), entryV.getValue(), e.getEdgePhi());
								}
								// if there is no overlap between edges continue
								if (edgeOverlap == 0) {
									continue;
								}
								// approximation shows that the edge provides no value
								else if (edgeWeight - edgeOverlap < 0) {
									edgeWeight = 0;
									break;
								}
								// if there is overlap, update new edge weight
								else {
									edgeWeight = edgeWeight - edgeOverlap;
								}
							}
							// create the edge with the remaining weight, unless its 0 then the edge does not exist
							if (edgeWeight != 0) {
								GraphEdge newEdge = new GraphEdge(newVertex.getVertexID(), gv.getVertexID(), entryV.getKey(), entryV.getValue(), edgeWeight);
								foundEdges.add(newEdge);
							}
							int noItemsRho = entryV.getKey().getNoItems();
							noItemsRho = noItemsRho - result;
							// check if below 0 due to approximations
							if (noItemsRho < 0) {
								noItemsRho = 0;
							}
							// decrease the number of items in the rho
							entryV.getKey().setNoItems(noItemsRho);
							
							// if the rho is empty after this subtraction, set it to remote and stop analyzing it
							if (entryV.getKey().getNoItems() == 0) {
								//System.out.println("Alert: Rho is now empty, setting it to remote!");
								entryV.getKey().setRemote();
								break;
							} 
						}
					}
					// if the new vertex's rho under analysis is already empty it does not need further analysis
					if (entryV.getKey().isRemote())
						break;
				}
			}
		}
		
		// need to compute rho weight on a per table basis, first group them together
		HashMap<Integer, ArrayList<Pair<VertexRho, VertexPhi>>> groupedRhos = groupRhos(rhosV);
		// variable to store vertex weight
		int rhosWeight = 0;
		// iterate over all tables and get their weight
		for (Map.Entry<Integer, ArrayList<Pair<VertexRho, VertexPhi>>> table : groupedRhos.entrySet()) {
			// get the weight of the table
			System.out.println("Number of rhos to analyze in table " + table.getKey() + " is: " + table.getValue().size());
			int tableWeight = computeTableWeight(table.getValue());
			// add weight of table to rho weight
			System.out.println("Table " + table.getKey() + " weight: " + tableWeight);
			rhosWeight += tableWeight;
		}
		// need to remove weight corresponding to the remote items, group edges according to rho table
		HashMap<Integer, ArrayList<Pair<VertexRho, VertexPhi>>> groupedEdgeRhos = new HashMap<>();
		for (GraphEdge e : foundEdges) {
			VertexRho edgeRho = e.getEdgeRho();
			VertexPhi edgePhi = e.getEdgePhi();
			int table = Integer.parseInt(edgeRho.getTable());
			// if table exists add rho to it
			if (groupedEdgeRhos.containsKey(table)) {
				Pair<VertexRho, VertexPhi> rhoPhi = new Pair<VertexRho, VertexPhi>(edgeRho, edgePhi);
				groupedEdgeRhos.get(table).add(rhoPhi);
			}
			else {
				Pair<VertexRho, VertexPhi> rhoPhi = new Pair<VertexRho, VertexPhi>(edgeRho, edgePhi);
				ArrayList<Pair<VertexRho, VertexPhi>> rhoPhis = new ArrayList<>();
				rhoPhis.add(rhoPhi);
				groupedEdgeRhos.put(table, rhoPhis);
			}
		}
		// variable to store remote item count
		int remoteWeight = 0;
		// iterate over all tables and get their weight
		for (Map.Entry<Integer, ArrayList<Pair<VertexRho, VertexPhi>>> table : groupedEdgeRhos.entrySet()) {
			// get the weight of the table
			int tableWeight = computeTableWeight(table.getValue());
			// add weight of table to rho weight
			System.out.println("Table " + table.getKey() + " remote items: " + tableWeight);
			remoteWeight += tableWeight;
		}
		
		int vertexWeight = rhosWeight - remoteWeight;
		
		
		System.out.println("Rhos weight for vertex is " + vertexWeight);
			
			
		// in this stage the new vertex has been compared and updated regarding all previous vertices
		graph.put(newVertex, new ArrayList<>());
		// add its edges to the graph as well
		for (GraphEdge e : foundEdges) {
			addEdge(newVertex, e, graph);
		}
		// compute vertex Weight
		newVertex.computeVertexWeightSMT(vertexWeight);
		System.out.println("Subvertex added successfully");
	}
	
	// method that computes the weight of a given rho using aproxmc 
	
	// method that computes rho weight given its phi
	private int computeRhoWeight(VertexRho rho, VertexPhi phi) {
		// obtain strings for query
		String rho1 = rho.getRho(); 
		String phi1 = phi.getPhiAsString(); 
		HashSet<String> vars = rho.getVariables(); 
		
		// create SMT file
		String fileName = "SMTfileVertex.smt2";
		
		File smtFile = new File(fileName);
		try {
			smtFile.createNewFile();
		} catch (IOException e) {
			System.out.println("Error while creating SMT file!");
			e.printStackTrace();
		}
		// write to SMT file
		try {
			// reset tags for SMT vars in file
			smtTag = 0;
			
			FileWriter smtWritter = new FileWriter(fileName);
			// first line contains smt lib headers
			smtWritter.append("(set-option :count-models true)\n");
			smtWritter.append("(set-logic QF_BV)\n");
			smtWritter.append("(set-option :print-clauses-file \"./counts.cnf\")");
			smtWritter.append("(set-info :smt-lib-version 2.0)\n");
			smtWritter.append("(declare-fun rho_1 () (_ BitVec 32))\n");
			smtWritter.append("(declare-fun rho_2 () (_ BitVec 32))\n");
			smtWritter.append("(declare-fun phi_1 () Bool)\n");
			smtWritter.append("(declare-fun phi_2 () Bool)\n");
			
			for (String variable : vars) {
				smtWritter.append("(declare-fun " + variable + " () (_ BitVec 32))\n");
			}
			
			smtWritter.append("(assert (= rho_1 " + recursiveRhoToSMT(rho1.substring(rho1.indexOf(">") + 1), true) + "))\n");
			if (rho.tableSplitExists()) {
				smtWritter.append(tableSplitSMT(rho, 1));
			}
			smtWritter.append("(assert (= rho_2 " + recursiveRhoToSMT(rho1.substring(rho1.indexOf(">") + 1), false) + "))\n");

			
			smtWritter.append("(assert (= phi_1 " + parsePhiIntoSmt(phi1) + "))\n");
			smtWritter.append("(assert (= phi_2 " + parsePhiIntoSmt(phi1) + "))\n");

			
			smtWritter.append("(assert (and (= rho_1 rho_2) (and phi_1 phi_2)))\n");

			
			String varsCounting = "";
			for (int i = 0; i < vars.size(); i++) {
				varsCounting += "v" + (i + 1) + " ";
			}
			varsCounting = varsCounting.substring(0, varsCounting.length() - 1);
			smtWritter.append("(count-models " + varsCounting + ")\n");
			smtWritter.append("(exit)");
			// close file
			smtWritter.close();
			
			// SMT file is built, only need to count number of solutions using approxmc and opensmt			
			Runtime rt = Runtime.getRuntime();
			String[] commandsSMT = {"./resources/opensmt", "SMTfileVertex.smt2"};
			Process proc = rt.exec(commandsSMT);

			BufferedReader stdInput = new BufferedReader(new 
			     InputStreamReader(proc.getInputStream()));

			BufferedReader stdError = new BufferedReader(new 
			     InputStreamReader(proc.getErrorStream()));
			
			
			// Read the output from the command
			String s = null;
			while ((s = stdInput.readLine()) != null) {
			    //System.out.println(s);
			}
			// Read any errors from the attempted command
			while ((s = stdError.readLine()) != null) {
				System.out.println("Error during openSMT phase - rho weight");
			    System.out.println(s);
				System.exit(0);
			}
			
			String[] commandsApprox = {"./resources/approxmc", "counts.cnf", "--epsilon=0.8", "--delta=0.4"};
			proc = rt.exec(commandsApprox);
			int rhoWeight = 0;
			
			stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));

			stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
			
			s = null;
			while ((s = stdInput.readLine()) != null) {
				if (s.startsWith("s mc")) {
			    	String[] splits = s.split(" ");
			    	rhoWeight = Integer.parseInt(splits[2]);
			    	//System.out.println("Rho weight: " + rhoWeight);
			    	return rhoWeight;
			    }
			}
			// Read any errors from the attempted command
			while ((s = stdError.readLine()) != null) {
				System.out.println("Error during ApproxMC phase");
			    System.out.println(s);
			}
			
		} catch (IOException e) {
			System.out.println("Error on generating SMT file!");
			e.printStackTrace();
		}
		
	
		
		return 0;
	}
	
	// method that computes the weight of a table in a given vertex
	
	// methat that computes table weight
	private int computeTableWeight(ArrayList<Pair<VertexRho, VertexPhi>> tableRhos) {
		
		// list of variables in vertex
		HashSet<String> varsVertex = new HashSet<>();
		// variable declarations
		ArrayList<String> varDeclarations = new ArrayList<>();
		// rho and phi declarations
		ArrayList<Pair<String, String>> rhoPhiDeclarations = new ArrayList<>();
		// table split declarations
		ArrayList<String> tableSplitDeclarations = new ArrayList<>();
		
		// create SMT file
		String fileName = "SMTfileVertex.smt2";
		
		File smtFile = new File(fileName);
		try {
			smtFile.createNewFile();
		} catch (IOException e) {
			System.out.println("Error while creating SMT file!");
			e.printStackTrace();
		}
		
		// write to SMT file
		try {
			// reset tags for SMT vars in file
			smtTag = 0;
			
			FileWriter smtWritter = new FileWriter(fileName);
			// first line contains smt lib headers
			smtWritter.append("(set-option :count-models true)\n");
			smtWritter.append("(set-logic QF_BV)\n");
			smtWritter.append("(set-option :print-clauses-file \"./counts.cnf\")");
			smtWritter.append("(set-info :smt-lib-version 2.0)\n");
			
			// iterator for rho index
			int index = 1;
			// need to compute vertex weight based on all its rhos
			for (Pair<VertexRho, VertexPhi> entry: tableRhos) {
				smtWritter.append("(declare-fun rho_" + index + " () (_ BitVec 32))\n");
				smtWritter.append("(declare-fun phi_" + index + " () Bool)\n");
				
				// obtain strings for query
				String rho1 = entry.getKey().getRho(); 
				String phi1 = entry.getValue().getPhiAsString(); 
				HashSet<String> vars = entry.getKey().getVariables();
				
				// add new variables to query if appliable
				for (String variable : vars) {
					if (!varsVertex.contains(variable)) {
						varDeclarations.add("(declare-fun " + variable + " () (_ BitVec 32))\n");
						varsVertex.add(variable);
					}
				}
				// add rho declarations
				String rhoDeclaration = "(assert (= rho_" + index + " " + recursiveRhoToSMT(rho1.substring(rho1.indexOf(">") + 1), false) + "))\n";
				// add table splti declaration if appliable
				if (entry.getKey().tableSplitExists()) {
					tableSplitDeclarations.add(tableSplitSMT(entry.getKey(), index));
				}
				// add phi declaration
				String phiDeclaration = "(assert (= phi_" + index + " " + parsePhiIntoSmt(phi1) + "))\n";
				// add both rho and phi declaration 
				Pair<String, String> rhoPhi = new Pair<String, String>(rhoDeclaration, phiDeclaration);
				rhoPhiDeclarations.add(rhoPhi);
				index++;
			}
			// add variable declarations to SMT file
			for (String varDeclaration : varDeclarations) {
				smtWritter.append(varDeclaration);
			}
			// add a test variable declaration
			smtWritter.append("(declare-fun test () (_ BitVec 32))\n");
			// add rho phi declarations
			for (Pair<String, String> rhoPhiDeclaration : rhoPhiDeclarations) {
				smtWritter.append(rhoPhiDeclaration.getKey());
				smtWritter.append(rhoPhiDeclaration.getValue());
			}
			// add table split declarations
			for (String tableSplitDeclaration : tableSplitDeclarations) {
				smtWritter.append(tableSplitDeclaration);
			}
			
			// build the phi query
			String phiQuery = "";
			for (int i = 0; i < rhoPhiDeclarations.size(); i ++) {
				if (i == 0) {
					phiQuery = "phi_1";
				}
				else {
					phiQuery = "(and " + phiQuery + " phi_" + (i + 1) + ")";
				}
			}
			phiQuery = "(assert " + phiQuery + ")\n";
			smtWritter.append(phiQuery);
			
			// build the assert query
			String assertQuery = "";
			for (int i = 0; i < rhoPhiDeclarations.size(); i++) {
				if (i == 0) {
					assertQuery += " (= (! test :named v1) rho_1)";
				}
				else {
					assertQuery = " (or" + assertQuery + "(= test rho_" + (i + 1) + "))";
				}
			}
			assertQuery = "(assert" + assertQuery + ")\n";
			smtWritter.append(assertQuery);
			smtWritter.append("(count-models v1)\n");
			
			smtWritter.append("(exit)");
			// close file
			smtWritter.close();
			
			// SMT file is built, only need to count number of solutions using approxmc and opensmt			
			Runtime rt = Runtime.getRuntime();
			String[] commandsSMT = {"./resources/opensmt", "SMTfileVertex.smt2"};
			Process proc = rt.exec(commandsSMT);

			BufferedReader stdInput = new BufferedReader(new 
			     InputStreamReader(proc.getInputStream()));

			BufferedReader stdError = new BufferedReader(new 
			     InputStreamReader(proc.getErrorStream()));
			
			
			// Read the output from the command
			String s = null;
			while ((s = stdInput.readLine()) != null) {
			    //System.out.println(s);
			}
			// Read any errors from the attempted command
			while ((s = stdError.readLine()) != null) {
				System.out.println("Error during openSMT phase - rho weight");
			    System.out.println(s);
				System.exit(0);
			}
			
			String[] commandsApprox = {"./resources/approxmc", "counts.cnf", "--epsilon=0.8", "--delta=0.4"};
			proc = rt.exec(commandsApprox);
			int rhoWeight = 0;
			
			stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));

			stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
			
			s = null;
			while ((s = stdInput.readLine()) != null) {
				if (s.startsWith("s mc")) {
			    	String[] splits = s.split(" ");
			    	rhoWeight = Integer.parseInt(splits[2]);
			    	//System.out.println("Rho weight: " + rhoWeight);
			    	return rhoWeight;
			    }
			}
			// Read any errors from the attempted command
			while ((s = stdError.readLine()) != null) {
				System.out.println("Error during ApproxMC phase");
			    System.out.println(s);
			}
			
		} catch (IOException e) {
			System.out.println("Error on generating SMT file!");
			e.printStackTrace();
		}
		return 0;
	}
	
	
	// method that computes the weight of the intersection between two rhos given each associated phi
	private int rhoIntersection(VertexRho rhoV, VertexRho rhoGV, VertexPhi phiV, VertexPhi phiGV) {
		// obtain strings for query		
		String rho1 = rhoV.getRho(); // new vertex
		String rho2 = rhoGV.getRho(); // old vertex
		
		rho2 = variableRename(rho2);
		
		String phi1 = phiV.getPhiAsString(); // new vertex
		String phi2 = phiGV.getPhiAsString(); // old vertex
		
		phi2 = variableRename(phi2);
		
		HashSet<String> vars1 = rhoV.getVariables(); // new vertex
		HashSet<String> vars2 = rhoGV.getVariables(); // old vertex
		
		if (!checkIntersection(new Pair<VertexRho, VertexPhi>(rhoV, phiV), new Pair<VertexRho, VertexPhi>(rhoGV, phiGV))) {
			return 0;
		}
		
		
		// create SMT file
		String fileName = "SMTfile.smt2";
		File smtFile = new File(fileName);
		try {
			smtFile.createNewFile();
		} catch (IOException e) {
			System.out.println("Error while creating SMT file!");
			e.printStackTrace();
		}
		// write to SMT file
		try {
			// reset tags for SMT vars in file
			smtTag = 0;
			
			FileWriter smtWritter = new FileWriter(fileName);
			// first line contains smt lib headers
			smtWritter.append("(set-option :count-models true)\n");
			smtWritter.append("(set-logic QF_BV)\n");
			smtWritter.append("(set-option :print-clauses-file \"./counts.cnf\")");
			smtWritter.append("(set-info :smt-lib-version 2.0)\n");
			smtWritter.append("(declare-fun rho_1 () (_ BitVec 32))\n");
			smtWritter.append("(declare-fun rho_2 () (_ BitVec 32))\n");
			smtWritter.append("(declare-fun phi_1 () Bool)\n");
			smtWritter.append("(declare-fun phi_2 () Bool)\n");
			
			for (String variable : vars1) {
				smtWritter.append("(declare-fun " + variable + " () (_ BitVec 32))\n");
			}
			for (String variable : vars2) {
				variable = variable.replaceAll("id", "idGV");
				smtWritter.append("(declare-fun " + variable + " () (_ BitVec 32))\n");
			}
			
			smtWritter.append("(assert (= rho_1 " + recursiveRhoToSMT(rho1.substring(rho1.indexOf(">") + 1), true) + "))\n");
			if (rhoV.tableSplitExists()) {
				smtWritter.append(tableSplitSMT(rhoV, 1));
			}
			smtWritter.append("(assert (= rho_2 " + recursiveRhoToSMT(rho2.substring(rho2.indexOf(">") + 1), false) + "))\n");
			if (rhoGV.tableSplitExists()) {
				smtWritter.append(tableSplitSMT(rhoGV, 2));
			}
			smtWritter.append("(assert (= phi_1 " + parsePhiIntoSmt(phi1) + "))\n");
			smtWritter.append("(assert (= phi_2 " + parsePhiIntoSmt(phi2) + "))\n");

			
			smtWritter.append("(assert (and (= rho_1 rho_2) (and phi_1 phi_2)))\n");
			
			String varsCounting = "";
			for (int i = 0; i < vars1.size(); i++) {
				varsCounting += "v" + (i + 1) + " ";
			}
			varsCounting = varsCounting.substring(0, varsCounting.length() - 1);
			smtWritter.append("(count-models " + varsCounting + ")\n");
			smtWritter.append("(exit)");
			// close file
			smtWritter.close();
			fileId++;
						
			// SMT file is built, only need to count number of solutions using approxmc and opensmt
			Runtime rt = Runtime.getRuntime();
			String[] commandsSMT = {"./resources/opensmt", "SMTfile.smt2"};
			Process proc = rt.exec(commandsSMT);

			BufferedReader stdInput = new BufferedReader(new 
			     InputStreamReader(proc.getInputStream()));

			BufferedReader stdError = new BufferedReader(new 
			     InputStreamReader(proc.getErrorStream()));
			
			
			// Read the output from the command
			String s = null;
			while ((s = stdInput.readLine()) != null) {
			    //System.out.println(s);
			}
			// Read any errors from the attempted command
			while ((s = stdError.readLine()) != null) {
				System.out.println("Error during openSMT phase - edge weight");
			    System.out.println(s);
			    System.exit(0);
			}
			
			String[] commandsApprox = {"./resources/approxmc", "counts.cnf", "--epsilon=0.8", "--delta=0.4"};
			proc = rt.exec(commandsApprox);
			int intersectionSize = 0;
			
			stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));

			stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
			
			s = null;
			while ((s = stdInput.readLine()) != null) {
				if (s.startsWith("s mc")) {
			    	String[] splits = s.split(" ");
			    	intersectionSize = Integer.parseInt(splits[2]);
			    	//System.out.println("Intersection size: " + intersectionSize);
			    	return intersectionSize;
			    }
			}
			// Read any errors from the attempted command
			while ((s = stdError.readLine()) != null) {
				System.out.println("Error during ApproxMC phase");
			    System.out.println(s);
			}	
			
		} catch (IOException e) {
			System.out.println("Error on generating SMT file!");
			e.printStackTrace();
		} 
		
		return 0;
	}
	
	// method that builds extra smt line for table splits
	private String tableSplitSMT(VertexRho rho, int rhoNumber) {
		// get lower table limit
		int lower = rho.getLowerTableLimit() - 1;
		// get upper table limit
		int upper = rho.getUpperTableLimit();
		// convert lower to binary
		String lowerBin = Integer.toBinaryString(lower);
		// convert upper to binary
		String upperBin = Integer.toBinaryString(upper);
		// build formatted string
		if (lower != -1) {
			return "(assert (and (bvult " + countAndFixBin(lowerBin) + " rho_" 
						+ rhoNumber + ") (bvult rho_" 
						+ rhoNumber + " "+ countAndFixBin(upperBin) + ")))\n"; 
		}
		else {
			return "(assert (bvult rho_" + rhoNumber + " " + countAndFixBin(upperBin) + "))\n";
		}
	}
	
	// method that converts phis into smt lib format
	private String parsePhiIntoSmt(String phi) {
		
		String[] parts = phi.split("&&");
		
		String smtString = "";
		
		String memberCounting = "";
		for (int i = 0; i < parts.length - 1; i++) {
			memberCounting += "(and ";
		}
		
		smtString += memberCounting;
		
		for (String part: parts) {
			String member = "(and (bvult ";
			
			String leftLimit = "";
			String variable = "";
			String rightLimit = "";
			boolean variableDone = false;
			
			for (int i = 0; i < part.length(); i++){ 
			    char c = part.charAt(i);
			    
				// left limit building
				if (Character.isDigit(c) && variable == "") {
			    	leftLimit += c;
			    }
				// right limit building
				else if (Character.isDigit(c) && variableDone) {
			    	rightLimit += c;
			    }
				// variable building
				else if (Character.isLetter(c) || (Character.isDigit(c) && !variableDone)) {
					variable += c;
				}
				//operators
				else if (c == '<' || c == '=') {
					continue;
				}
				// end of a member
				else if (c == ' ' || Character.isWhitespace(c)) {
					if (variable != "") {
						variableDone = true;
					}
					continue;
				}
			}
			
			int leftLimitInt = Integer.parseInt(leftLimit);
			
			if (leftLimitInt != 0) {
			
				String leftLimitBin = Integer.toBinaryString(Integer.parseInt(leftLimit) - 1);
				String rightLimitBin = Integer.toBinaryString(Integer.parseInt(rightLimit) + 1);
				
				member += countAndFixBin(leftLimitBin) + " " + variable + ") (bvult " + variable + " " + countAndFixBin(rightLimitBin) + "))";  
				
				smtString += member;
			}
			
			else {
				
				String rightLimitBin = Integer.toBinaryString(Integer.parseInt(rightLimit) + 1);

				member = "(bvult " + variable + " " + countAndFixBin(rightLimitBin) + ")";
				
				smtString += member;
			}
			
		}
		
		for (int i = 0; i < parts.length - 1; i++) {
			smtString += ")";
		}
		return smtString;
	}
	
	// method that coverts rho into smt lib format
	private String recursiveRhoToSMT(String rho, boolean rho1) {
		// find splitting index related to external operation
		int splitIndex = findExternalOperation(rho);
		
		// if splitIndex is -1 then we reached an operand
		if (splitIndex == -1) {
			return findOperand(rho, rho1);
		}		
		// find which operation has been found as external
		if (rho.charAt(splitIndex) == '+') {
			return "(bvadd " + recursiveRhoToSMT(rho.substring(1, splitIndex - 1), rho1) + " " 
					+ recursiveRhoToSMT(rho.substring(splitIndex + 2, rho.length() - 1), rho1) + ")";
		}
		else if (rho.charAt(splitIndex) == '*') {
			return "(bvmul " + recursiveRhoToSMT(rho.substring(1, splitIndex - 1), rho1) + " " 
					+ recursiveRhoToSMT(rho.substring(splitIndex + 2, rho.length() - 1), rho1) + ")";
		}
		else if (rho.charAt(splitIndex) == '-') {
			return "(bvsub " + recursiveRhoToSMT(rho.substring(1, splitIndex - 1), rho1) + " " 
					+ recursiveRhoToSMT(rho.substring(splitIndex + 2, rho.length() - 1), rho1) + ")";
		}
		
		// shouldn't happen
		System.out.println("Buggy conversion from rho to smt rho");
		return null;
	}
	
	// method that finds an operand and prints in SMT format
	private String findOperand(String rho, boolean rho1) {
		
		
		// build the operand string
		String operand = "";
		for (int i = 0; i < rho.length(); i++){
			// read each char
		    char c = rho.charAt(i);
		    // check if next char is a letter or a digit
		    if (Character.isLetter(c) || Character.isDigit(c)) {
		    	operand += c;
		    }
		    else {
				System.out.println(rho);
		    	System.out.println("Unknown operand character");
		    	System.out.println(c);
		    }
		}
		// check if operand is a number or a variable
		if (Character.isDigit(operand.charAt(0))) {
			String operandBin = Integer.toBinaryString(Integer.parseInt(operand));
			return countAndFixBin(operandBin);
		}
		else {
			// check if its related to rho1 or rho2
			if (rho1) {
				smtTag++;
				return "(! " + operand + " :named v" + smtTag + ")";
			}

			return operand;
		}

	}
	
	// method that finds an operand and prints in SMT LIA format
	private String findOperand(String rho) {
		// build the operand string
		String operand = "";
		for (int i = 0; i < rho.length(); i++){
			// read each char
		    char c = rho.charAt(i);
		    // check if next char is a letter or a digit
		    if (Character.isLetter(c) || Character.isDigit(c)) {
		    	operand += c;
		    }
		    else {
				System.out.println(rho);
		    	System.out.println("Unknown operand character");
		    	System.out.println(c);
		    }
		}
		return operand;

	}
	
	// method that finds external operation in rho, inclusion index = 1
	private int findExternalOperation(String rho) {
		// inclusion index to count for nested operations
		int inclIndex = 0;
		// iterate through rho and find external operation
		for (int i = 0; i < rho.length(); i++){
			// read each char
		    char c = rho.charAt(i);
		    // if its a parenthesis opening increase incl index
		    if (c == '(') {
		    	inclIndex++;
		    }
		    // if its a paranthesis closing decrease incl index
		    if (c == ')') {
		    	inclIndex--;
		    }
		    // if we found an operation with inclusion index == 1, return its index in rho
		    if ((c == '+' || c == '*' || c == '-') && inclIndex == 1) {
		    	return i;
		    }
		    else 
		    	continue;
		}
		// error situation, should be impossible, throws -1
		return -1;
	}
	
	// method to build 32-bit binary number
	private String countAndFixBin(String bin) {
		int count = 0;
		for (int i = 0, len = bin.length(); i < len; i++) {
		    if (Character.isDigit(bin.charAt(i))) {
		        count++;
		    }
		}
		String fixedString = new String(new char[32-count]).replaceAll("\0", "0");
		return "#b" + fixedString + bin;
	}

	// method used to add an edge to a graph
	private void addEdge(GraphVertex v, GraphEdge edge, LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph) {
		graph.get(v).add(edge);
	}
	
	// method used to build adjacency matrix for graph presenting
	private LinkedHashMap<Integer, Pair<Integer,HashMap<Integer, Integer>>> buildMETISGraph() {
		// final graph for METIS
		LinkedHashMap<Integer, Pair<Integer,HashMap<Integer, Integer>>> METISGraph = new LinkedHashMap<>();
		// iterate through every vertex
		for (Map.Entry<GraphVertex, ArrayList<GraphEdge>> vertex : splitGraph.entrySet()) {
			// map each edge destiny with a weight
			HashMap<Integer, Integer> edges = new HashMap<>();
			// go through all the edges linked to a vertex
			for (GraphEdge e : vertex.getValue()) {
				// get edge destiny
				int edgeDst = e.getDest();
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
			METISGraph.put(vertex.getKey().getVertexID(), new Pair<Integer, HashMap<Integer, Integer>>(vertex.getKey().getVertexWeight(),edges));
		}
		
		for (Map.Entry<Integer, Pair<Integer,HashMap<Integer, Integer>>> entry : METISGraph.entrySet()) {
			int edgeDest = entry.getKey();
			
			for (Map.Entry<Integer, Integer> edge : entry.getValue().getValue().entrySet()) {
				
				HashMap<Integer, Integer> graphEdges = METISGraph.get(edge.getKey()).getValue();
				
				if (graphEdges.containsKey(edgeDest)) {
					graphEdges.put(edgeDest, graphEdges.get(edgeDest) + edge.getValue());
				}
				else {
					this.noEdges++;
					graphEdges.put(edgeDest, edge.getValue());

				}
			}
		}
		
		// edges are bidirectional
		return METISGraph;
	}
	
	// method to print input file for METIS given a graph
	private void printMETISfile(LinkedHashMap<Integer, Pair<Integer, HashMap<Integer, Integer>>> graph) {
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
			metisFile.append(this.noVertices + " " +this.noEdges/2 + " " + "011\n");
			// iterate through all the vertices in the graph
			for (Map.Entry<Integer, Pair<Integer, HashMap<Integer, Integer>>> vertex : graph.entrySet()) {
				// get vertex weight
				int vertexWeight = vertex.getValue().getKey();
				// print vertex line
				metisFile.append(String.valueOf(vertexWeight));
				for (Map.Entry<Integer, Integer> edge : vertex.getValue().getValue().entrySet()) {
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
		
		// System.out.println("Checking for commmon splits: " + rhos.size());
		
		// possible splits
		HashSet<String> splits = new HashSet<>();
		// set of common variables in every rho on the same table
		HashSet<String> commonVars = new HashSet<>();
		// check commonVars accross all rhos
		boolean isFirst = true;
		for (Pair<VertexRho, VertexPhi> rho: rhos) {
			// skip low prob rhos
			if (rho.getKey().getProb() < 0.5) {
				continue;
			}
	
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
				// skip low probs rhos
				if (rho1.getKey().getProb() < 0.5) continue;
				// get range of commonVar
				Pair<Integer, Integer> varRange = rho1.getValue().getPhi().get(commonVar);
				
				/*
				System.out.println(commonVar);
				System.out.println("Lower: " + varRange.getKey());
				System.out.println("Upper : " + varRange.getValue());
				*/
				
				// calculate cutoff for new simulated phi
				int cutoff = (varRange.getValue() - varRange.getKey()) / 2;

				
				// variable range is 0 so this variable cannot be a split var
				if ((varRange.getValue() - (varRange.getKey())) == 0) {
					overlap = true;
					break;
				}
				
				VertexRho rhoCopy1 = new VertexRho(rho1.getKey());
				VertexPhi phiCopy1 = new VertexPhi(rho1.getValue());
				// edit phi for sim 
				phiCopy1.getPhi().put(commonVar, new Pair<Integer, Integer>(varRange.getKey(), cutoff));
								
				// build rho phi pair
				Pair<VertexRho, VertexPhi> rhoPhi1 = new Pair<VertexRho, VertexPhi>(rhoCopy1, phiCopy1);
				for (int j = 0; j < rhos.size(); j++) {
					// get rho
					Pair<VertexRho, VertexPhi> rho2 = rhos.get(j);
					// skip low prob rhos
					if (rho2.getKey().getProb() < 0.5) continue;
					VertexRho rhoCopy2 = new VertexRho(rho2.getKey());
					VertexPhi phiCopy2 = new VertexPhi(rho2.getValue());
					// edit phi for sim 
					phiCopy2.getPhi().put(commonVar, new Pair<Integer, Integer> (cutoff + 1, varRange.getValue()));
					
					//System.out.println("After 2: " + (cutoff + 1) + ", "  + (varRange.getValue())); 

					
					// build rho phi pair
					Pair<VertexRho, VertexPhi> rhoPhi2 = new Pair<VertexRho, VertexPhi>(rhoCopy2, phiCopy2);
					overlap = checkIntersection(rhoPhi1, rhoPhi2);
					
					/*
					System.out.println(overlap);
					System.out.println(rhoPhi1.getKey().getRho());
					System.out.println(rhoPhi1.getValue().getPhiAsGroup());
					System.out.println(rhoPhi2.getKey().getRho());
					System.out.println(rhoPhi2.getValue().getPhiAsGroup());
					*/
					
					
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
	
	// method that returns the splits used for a given vertex
	public LinkedHashMap<Integer, ArrayList<String>> getSplits() {
		return this.splits;
	}
	
	
}
	
	