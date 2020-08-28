package thesis;

import java.util.ArrayList;
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
			HashMap<String, Integer> variableCost = new HashMap<>();
			// obtain rho phi pair for the given vertex
			HashMap<VertexRho, VertexPhi> rhos = v.getSigma().getRhos();
			// group together rhos that belong to the same table within a vertex
			HashMap<Integer, ArrayList<Pair<VertexRho, VertexPhi>>> buckets = groupRhos(rhos);
			// after having rhos grouped by table, check for possible overlaps and rank the variables
			for (Map.Entry<Integer, ArrayList<Pair<VertexRho, VertexPhi>>> entry: buckets.entrySet()) {			
				// compare all the pairs with one another
				for (int i = 0; i < entry.getValue().size(); i++) {
					Pair<VertexRho, VertexPhi> rhoPhi1 = entry.getValue().get(i);
					// obtain accessed data items for given rho phi pair
					String list1 = checkAccess(rhoPhi1);
					// update variable ranking structure
					HashSet<String> variables1 = rhoPhi1.getKey().getVariables();
					variableCost = initializeRanking(rhoPhi1, variables1, variableCost);				
					// compare to all other rho phi pairs with same table
					for (int j = i + 1; j < entry.getValue().size(); j++) {
						Pair<VertexRho, VertexPhi> rhoPhi2 = entry.getValue().get(j);
						// obtain accessed data items for given rho phi pair
						String list2 = checkAccess(rhoPhi2);
						// update variable ranking structure
						HashSet<String> variables2 = rhoPhi2.getKey().getVariables();
						variableCost = initializeRanking(rhoPhi2, variables2, variableCost);
						// compute update of ranking structure based on intersection length
						variableCost = updateRanking(list1, list2, variables1, variables2, variableCost);
					} 
				}
			} 
			// at this stage we have computed the max possible penalty for each variable
			printRanking(variableCost, counter);			
			// obtain variable to split on which has the minimum possible penalty
			String splitVariable = getSplitVariable(variableCost, counter);
			// obtain split variable range in original vertex
			Pair<Integer, Integer> splitRange = getSplitRange(rhos, splitVariable);
			
			
			
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
			rho += " && " + rhoPhi.getKey().getRhoUpdate();
		}
		String phi = rhoPhi.getValue().getPhiAsGroup();
		
		String query = "Flatten[Table[ " + rho +  ", " + phi + "]]]";
		String list = link.evaluateToOutputForm(query, 0);
		return list;
	}

	private HashMap<String, Integer> initializeRanking(Pair<VertexRho, VertexPhi> rhoPhi, HashSet<String> variables, HashMap<String, Integer> variableCost) {
		for (String variable: variables) {
			if (!variableCost.containsKey(variable)) 
				variableCost.put(variable, 0);
		}
		return variableCost;
	}
	
	private HashMap<String, Integer> updateRanking(String l1, String l2, HashSet<String> v1, HashSet<String> v2, HashMap<String, Integer> variableCost) {
		KernelLink link = MathematicaHandler.getInstance();
		String queryIntersection = "Length[Intersection[" + l1 + ", " + l2 + "]]";
		String intersectionLength = link.evaluateToOutputForm(queryIntersection, 0);
		Integer intLength = Integer.parseInt(intersectionLength);
		if (intLength != 0) {
			// add possible cost to all variables involved 								
			for (String variable: v1) 
				variableCost.put(variable, variableCost.get(variable) + intLength);
			for (String variable: v2) 
				variableCost.put(variable, variableCost.get(variable) + intLength);
		}
		return variableCost;
	}

	private void printRanking(HashMap<String, Integer> variableCost, int counter) {
		System.out.println("Printing possible splits for vertex no " + counter);
		for (Map.Entry<String, Integer> entry: variableCost.entrySet()) {
			System.out.println("Variable: " + entry.getKey() + " -> " + entry.getValue());
		}
	}
	
	private String getSplitVariable(HashMap<String, Integer> variableCost, int counter) {
		int min = Integer.MAX_VALUE;
		String splitVariable = null;
		for(Map.Entry<String, Integer> entry : variableCost.entrySet()) {
			if (min > entry.getValue()) {
				min = entry.getValue();
				if (min == 0) {
					splitVariable = entry.getKey();
					break;
				}
				splitVariable  = entry.getKey();
			}
		}
		System.out.println("Split variable for vertex no " + counter + " -> " + splitVariable);
		return splitVariable;
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
	
}
