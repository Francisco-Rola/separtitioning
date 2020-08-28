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
		// for each Vertex, iterate through its rhos
		int counter = 1;
		for (GraphVertex v: graph.keySet()) {
			
			// store table of variables for each vertex
			HashMap<String, Integer> variableCost = new HashMap<>();
			
			HashMap<VertexRho, VertexPhi> rhos = v.getSigma().getRhos();
			// group together rhos that belong to the same table within a vertex
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
			// after having rhos grouped by table, check for possible overlaps and rank the variables
			for (Map.Entry<Integer, ArrayList<Pair<VertexRho, VertexPhi>>> entry: buckets.entrySet()) {			
				KernelLink link = MathematicaHandler.getInstance();
				// compare all the pairs with one another
				for (int i = 0; i < entry.getValue().size(); i++) {
					Pair<VertexRho, VertexPhi> rhoPhi1 = entry.getValue().get(i);
					String rho1 = rhoPhi1.getKey().getRho().substring(rhoPhi1.getKey().getRho().indexOf(">") + 1);
					if (rhoPhi1.getKey().getRhoUpdate() != null) {
						rho1 += " && " + rhoPhi1.getKey().getRhoUpdate();
					}
					String phi1 = rhoPhi1.getValue().getPhiAsGroup();
					
					String query1 = "Flatten[Table[ " + rho1 +  ", " + phi1 + "]]]";
					String list1 = link.evaluateToOutputForm(query1, 0);
					
					HashSet<String> variables1 = rhoPhi1.getKey().getVariables();
					for (String variable: variables1) {
						if (!variableCost.containsKey(variable)) 
							variableCost.put(variable, 0);
					}
					
					// compare to all other rho phi pairs with same table
					for (int j = i + 1; j < entry.getValue().size(); j++) {
						Pair<VertexRho, VertexPhi> rhoPhi2 = entry.getValue().get(j);
						String rho2 = rhoPhi2.getKey().getRho().substring(rhoPhi2.getKey().getRho().indexOf(">") + 1);
						if (rhoPhi2.getKey().getRhoUpdate() != null) {
							rho2 += " && " + rhoPhi2.getKey().getRhoUpdate();
						}
						String phi2 = rhoPhi2.getValue().getPhiAsGroup();
						
						String query2 = "Flatten[Table[ " + rho2 +  ", " + phi2 + "]]]";
						String list2 = link.evaluateToOutputForm(query2, 0);
						
						HashSet<String> variables2 = rhoPhi2.getKey().getVariables();
						for (String variable: variables2) {
							if (!variableCost.containsKey(variable)) 
								variableCost.put(variable, 0);
						}
						
						String queryIntersection = "Length[Intersection[" + list1 + ", " + list2 + "]]";
						String intersectionLength = link.evaluateToOutputForm(queryIntersection, 0);
						
						Integer intLength = Integer.parseInt(intersectionLength);
						
						if (intLength != 0) {
							// add possible cost to all variables involved 								
							for (String variable: variables1) 
								variableCost.put(variable, variableCost.get(variable) + intLength);
							for (String variable: variables2) 
								variableCost.put(variable, variableCost.get(variable) + intLength);
						}
					} // end of comparison between i and specific j
				} // end of i
			} // end of bucket
			
			// at this stage we have computed the max possible penalty for each variable
			System.out.println("Printing possible splits for vertex no " + counter);
			for (Map.Entry<String, Integer> entry: variableCost.entrySet()) {
				System.out.println("Variable: " + entry.getKey() + " -> " + entry.getValue());
			}
			counter++;
			
		}
		
	}
	

	
	


}
