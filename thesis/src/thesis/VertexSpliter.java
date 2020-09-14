package thesis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.wolfram.jlink.KernelLink;

import javafx.util.Pair;

public class VertexSpliter {
	
	private HashMap<GraphVertex, ArrayList<GraphEdge>> splitGraph = new HashMap<>();
	
	public VertexSpliter(HashMap<GraphVertex, ArrayList<GraphEdge>> graph) {
		// for each vertex, iterate through its rhos
		int counter = 1;
		for (GraphVertex v: graph.keySet()) {
			// store table of variables for each vertex, used for variable ranking
			HashMap<String, Pair<HashSet<Integer>, Integer>> ranking = new HashMap<>();
			// obtain rho phi pair for the given vertex
			HashMap<VertexRho, VertexPhi> rhos = v.getSigma().getRhos();
			// store no of rhos in V
			int noRhos = rhos.size();
			// initialize the variable ranking structure
			ranking = initRank(rhos, ranking);
			// group together rhos that belong to the same table within a vertex
			HashMap<Integer, ArrayList<Pair<VertexRho, VertexPhi>>> buckets = groupRhos(rhos);
			// after having rhos grouped by table, check for possible overlaps and rank the variables
			for (Map.Entry<Integer, ArrayList<Pair<VertexRho, VertexPhi>>> entry: buckets.entrySet()) {		
				// compare all the pairs with one another
				for (int i = 0; i < entry.getValue().size(); i++) {
					Pair<VertexRho, VertexPhi> rhoPhi1 = entry.getValue().get(i);
					// obtain accessed data items for given rho phi pair
					String list1 = checkAccess(rhoPhi1);
					HashSet<String> variables1 = rhoPhi1.getKey().getVariables();
					// check whether the rho function is injective for these inputs
					int injection = checkInjective(list1);
					// if the rho is not injective we are not interested in its variables
					if (injection != 0) {
						System.out.println("Non injective rho found -> " + rhoPhi1.getKey().getRho());
						System.out.println(list1);
						ranking = updateRank(variables1, ranking);
						continue;
					}
					// compare to all other rho phi pairs with same table
					for (int j = i + 1; j < entry.getValue().size(); j++) {
						Pair<VertexRho, VertexPhi> rhoPhi2 = entry.getValue().get(j);
						// obtain accessed data items for given rho phi pair
						String list2 = checkAccess(rhoPhi2);
						// update variable ranking structure
						HashSet<String> variables2 = rhoPhi2.getKey().getVariables();
						// check whether the rho function is injective for these inputs
						injection = checkInjective(list2);
						// if the rho is not injective we are not interested in its variables
						if (injection != 0) {
							System.out.println("Non injective rho found -> " + rhoPhi2.getKey().getRho());
							ranking = updateRank(variables2, ranking);
						}
						// compute update of ranking structure based on intersection length
						ranking = updateRank(list1, list2, variables1, variables2, ranking);
					} 
				}
			}
			// at this stage we have computed the max possible penalty for each variable
			printRanking(ranking , counter);			
			// obtain variable to split on which has the minimum possible penalty
			String splitVariable = getSplitVariable(ranking, noRhos, counter);
			// obtain split variable range in original vertex
			//Pair<Integer, Integer> splitRange = getSplitRange(rhos, splitVariable);
			//increment vertex counter
			counter++;
		}
		
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

	private String checkAccess(Pair<VertexRho, VertexPhi> rhoPhi) {
		KernelLink link = MathematicaHandler.getInstance();
		String rho = rhoPhi.getKey().getRho().substring(rhoPhi.getKey().getRho().indexOf(">") + 1);
		if (rhoPhi.getKey().getRhoUpdate() != null) {
			rho += " && !(" + rhoPhi.getKey().getRhoUpdate() + ")";
		}
		String phi = rhoPhi.getValue().getPhiAsGroup();
		
		String query = "Flatten[Table[ " + rho +  ", " + phi + "]]";
		String list = link.evaluateToOutputForm(query, 0);
		return list;
	}
	
	private int checkInjective(String list) {
		String[] items = list.split(",");
		int len = items.length;
		items = Arrays.stream(items).distinct().toArray(String[]::new);
		int newLen = items.length;
		return len - newLen;
	}

	private HashMap<String, Pair<HashSet<Integer>,Integer>> initRank(HashMap<VertexRho, VertexPhi> rhos, HashMap<String, Pair<HashSet<Integer>,Integer>> ranking) {
		int rhoID = 1;
		for (Map.Entry<VertexRho, VertexPhi> entry : rhos.entrySet()) {
			HashSet<String> vars = entry.getKey().getVariables();
			for (String var: vars) {
				if (!ranking.containsKey(var)) {
					HashSet<Integer> coverage = new HashSet<Integer>();
					coverage.add(rhoID);
					ranking.put(var, new Pair<HashSet<Integer>, Integer>(coverage,0));

				}
				else {
					HashSet<Integer> oldCoverage = ranking.get(var).getKey();
					oldCoverage.add(rhoID);
					int oldPenalty = ranking.get(var).getValue();
					ranking.put(var, new Pair<HashSet<Integer>, Integer>(oldCoverage, oldPenalty));
				}
			}
			rhoID++;
		}
		return ranking;
	}
	
	private HashMap<String, Pair<HashSet<Integer>,Integer>> updateRank(String l1, String l2, HashSet<String> v1, HashSet<String> v2, HashMap<String, Pair<HashSet<Integer>,Integer>> ranking) {
		KernelLink link = MathematicaHandler.getInstance();
		String queryIntersection = "Length[Intersection[" + l1 + ", " + l2 + "]]";
		String intersectionLength = link.evaluateToOutputForm(queryIntersection, 0);
		Integer intLength = Integer.parseInt(intersectionLength);
		if (intLength != 0) {
			// add possible cost to all variables involved 		
			for (String var: v1) {
				if (ranking.get(var).getValue() != Integer.MAX_VALUE) {
					HashSet<Integer> oldCoverage = ranking.get(var).getKey();
					int oldPenalty = ranking.get(var).getValue();
					ranking.put(var, new Pair<HashSet<Integer>,Integer>(oldCoverage, oldPenalty + intLength));
				}
			}
			for (String var: v2) {
				if (ranking.get(var).getValue() != Integer.MAX_VALUE) {
					HashSet<Integer>oldCoverage = ranking.get(var).getKey();
					int oldPenalty = ranking.get(var).getValue();
					ranking.put(var, new Pair<HashSet<Integer>,Integer>(oldCoverage, oldPenalty + intLength));
				}
			}
		}
		return ranking;
	}
	
	private HashMap<String, Pair<HashSet<Integer>,Integer>> updateRank(HashSet<String> vars, HashMap<String, Pair<HashSet<Integer>,Integer>> ranking) {
		// add max cost to all variables in non injective rho
		for (String var: vars) {
			HashSet<Integer> oldCoverage = ranking.get(var).getKey();
			ranking.put(var, new Pair<HashSet<Integer>,Integer>(oldCoverage, Integer.MAX_VALUE));
		}
		return ranking;
	}

	private void printRanking(HashMap<String, Pair<HashSet<Integer>, Integer>> ranking, int counter) {
		System.out.println("Printing possible splits for vertex no " + counter);
		for (Map.Entry<String, Pair<HashSet<Integer>,Integer>> entry: ranking.entrySet()) {
			System.out.println("Variable: " + entry.getKey() + " -> Coverage: " + entry.getValue().getKey().size() 
					+ " | Penalty: " + entry.getValue().getValue());
		}
	}
	
	private String getSplitVariable(HashMap<String, Pair<HashSet<Integer>,Integer>> ranking, int noRhos, int counter) {
		// map between rhoID and variables that split it
		HashMap<Integer, HashSet<String>> rhosCoverage = new HashMap<>();
		
		// initialize all rho's with an empty set of split variables
		for (int i = 1; i < noRhos + 1; i ++) {
			HashSet<String> vars = new HashSet<>();
			rhosCoverage.put(i, vars);
		}
		
		// find out which variables split each rho
		for (Map.Entry<String, Pair<HashSet<Integer>, Integer>> entry : ranking.entrySet()) {
			HashSet<Integer> rhosCovered = entry.getValue().getKey();
			for (Integer rhoID: rhosCovered) {
				rhosCoverage.get(rhoID).add(entry.getKey());
			}
		}
		
		// list of variables that are unique possible splitters for any rho (all of these need to be picked)
		HashSet<String> mustSplitVariables = new HashSet<>();
		
		// obtain must split variables
		for (Map.Entry<Integer, HashSet<String>> entry: rhosCoverage.entrySet()) {
			if (entry.getValue().size() == 1) {
				mustSplitVariables.add(entry.getValue().iterator().next());
			}
		}
		
		System.out.println("Must split variables for vertex no: " + counter + " -> " + mustSplitVariables);
		
		// initialize rho coverage given by must split vars
		HashSet<Integer> rhosCovered = new HashSet<>();
		
		// obtain coverage of must split variables combined
		for (String var: mustSplitVariables) {
			HashSet<Integer> rhosCoveredByVar = ranking.get(var).getKey();
			rhosCovered.addAll(rhosCoveredByVar);
		}
		
		System.out.println("Number of rhos in vertex no " + counter + ": " + noRhos + " -> " +
				"Coverage: " + rhosCovered.size());
				
		return null;
	}
	
	private Pair<Integer, Integer> getSplitRange(HashMap<VertexRho, VertexPhi> rhos, String splitVariable) {
		for (Map.Entry<VertexRho, VertexPhi> entry : rhos.entrySet()) {
			if (entry.getKey().getVariables().contains(splitVariable)) {
				System.out.println("Split variable range -> " + entry.getValue().getPhi().get(splitVariable));
				return entry.getValue().getPhi().get(splitVariable);
			}
		}
		return null;
	}
	
	private void splitVertex(String splitVariable, Pair<Integer, Integer> splitRange) {
		
	}
	
	
	
}
