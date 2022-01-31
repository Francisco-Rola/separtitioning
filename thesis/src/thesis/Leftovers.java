package thesis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import com.wolfram.jlink.KernelLink;

public class Leftovers {
	
	/*
	 // method that adds a vertex to the graph, ensuring no vertex overlaps
	private void addVertex(GraphVertex newVertex, LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph) {
		// edges found during rho comparison
		ArrayList<GraphEdge> foundEdges = new ArrayList<>();
		// need to compare newly added vertex to every other vertex
		HashMap<VertexRho, VertexPhi> rhosV = newVertex.getSigma().getRhos();
		// compare rho by rho
		for (Map.Entry<VertexRho, VertexPhi> entryV: rhosV.entrySet()) {
			String rhoV = entryV.getKey().getRho();
			// handle prob rhos later
			if (entryV.getKey().getProb() < 0.6) continue;
			// if replication is enable and read only table, dont compare
			if (replication && VertexPhi.checkTableReadOnly(Integer.parseInt(rhoV.substring(0, rhoV.indexOf(">") - 1)))) {
				continue;
			}
			// Do not consider index cost
			if (Integer.parseInt(rhoV.substring(0, rhoV.indexOf(">") - 1)) > 9) continue;
			
			// compare new vertex to all vertices in the graph to ensure they are disjoint 
			for (Map.Entry<GraphVertex, ArrayList<GraphEdge>> node : graph.entrySet()) {
				GraphVertex gv = node.getKey();
				// obtain all rhos in previously existing vertex
				HashMap<VertexRho, VertexPhi> rhosGV = gv.getSigma().getRhos();
				for (Map.Entry<VertexRho, VertexPhi> entryGV: rhosGV.entrySet()) {
					// skip low prob rho
					if (entryGV.getKey().getProb() < 0.5) {
						continue;
					}
					String rhoGV = entryGV.getKey().getRho();
					String phiGV = entryGV.getValue().getPhiAsString();
					// if rhos are not on same table they do need to be compared
					if (!rhoV.substring(0, rhoV.indexOf(">") - 1).equals(rhoGV.substring(0, rhoGV.indexOf(">") - 1))
							|| entryGV.getKey().isRemote()) {
						continue;
					}
					//compute intersection between rhos given the phis
					String result = null;
					String phiV = entryV.getValue().getPhiAsString();
					String phiVQ = preparePhi(phiV, entryV.getKey().getRhoUpdate());
					String phiGVQ = preparePhi(phiGV, entryGV.getKey().getRhoUpdate());
					result = rhoIntersection(rhoV, rhoGV, phiVQ, phiGVQ, entryV.getKey().getVariables(), entryGV.getKey().getVariables());
					// check the intersection results
					if (result.equals("False")) {
						// no overlap so no subtraction needed, simply update weight
						continue;
					}
					else if (result.equals("Format")) {
						// overlap detected but wrong format, need by hand changes
						String phiUpdated = null;
						int weight = VertexPhi.getScalingFactorI() / 2;
						for (Map.Entry<String, Pair<Integer, Integer>> phiToUpdate: entryV.getValue().getPhi().entrySet()) {
							if (phiToUpdate.getKey().startsWith("oliid")) {
								phiUpdated = phiToUpdate.getKey();
								Pair<Integer, Integer> newRange = new Pair<>(0, 0);
								entryV.getValue().getPhi().put(phiUpdated, newRange);
								break;
							}
						}
						String update = phiUpdated + " == 0";
						entryV.getKey().updateRho(update);
						
						GraphEdge edgeSrcV = new GraphEdge(newVertex.getVertexID(), gv.getVertexID(),rhoV, 
								weight, entryV.getKey().getProb(), entryV.getKey().getValue());
						foundEdges.add(edgeSrcV);
						entryV.getKey().checkRemoteAfterUpdate(entryV.getKey(), entryV.getValue());
						if (entryV.getKey().isRemote()) {
							System.out.println("Remote rho, removing");
							break;
						}
						
					}
					else {
						// collision found, perform rho logical subtraction
						entryV.getKey().updateRho(result);
						// add edge between vertices whose rhos-phi overlapped
						GraphEdge edgeSrcV = new GraphEdge(newVertex.getVertexID(), gv.getVertexID(),rhoV, 
								entryV.getKey().getRhoUpdate(), entryV.getValue().getPhiAsGroup(), entryV.getKey().getProb(), entryV.getKey().getValue());
						if (edgeSrcV.getEdgeWeight() != 0) foundEdges.add(edgeSrcV);
						// check if the rho is now fully remote after the update
						entryV.getKey().checkRemoteAfterUpdate(entryV.getKey(), entryV.getValue());
						if (entryV.getKey().isRemote())
							break;
					}
				}
				// if the new vertex's rho under analysis is already empty it does not need further analysis
				if (entryV.getKey().isRemote())
					break;
			}
		}
		// in this stage the new vertex has been compared and updated regarding all previous vertices
		graph.put(newVertex, new ArrayList<>());
		// add its edges to the graph as well
		for (GraphEdge e : foundEdges) {
			addEdge(newVertex, e, graph);
		}
		System.out.println("Subvertex added successfully");
	}
	 */
	
	//method that prepares phi for mathematica query, appending any update if needed
	/*
	private String preparePhi(String phi, String update) {
		String preparedPhi = new String(phi);
		if (update != null) {
			// if there is a constraint on rho, phi needs an update
			 preparedPhi = "(" + phi +  " && (" + update + "))";
		}
		return preparedPhi;
	}*/
	
	/*
	 // method that computes rho intersection and outputs their symbolic intersection
	private String rhoIntersection(String rho1, String rho2, String phi1, String phi2, HashSet<String> vars1, HashSet<String> vars2) {
		
		
		KernelLink link = MathematicaHandler.getInstance();
		
		rho2 = variableRename(rho2);
		phi2 = variableRename(phi2);
		
		//System.out.println(rho1);
		//System.out.println(phi1);
		
		//System.out.println(rho2);
		//System.out.println(phi2);
		
		
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
		Instant start = Instant.now();
		// build mathematica query with simplifier
		String query = "Reduce[" + "Simplify[(" + rhoQuery + ") && ("  + phiQuery + ")]" + ", " 
					+ variables + ", Integers, Backsubstitution -> True]";	
		
		String result = link.evaluateToOutputForm(query, 0);
		Instant end = Instant.now();
		if (Duration.between(start, end).toMillis() > 100) {
			System.out.println("Intersection computation time: " + Duration.between(start, end).toMillis());
			System.out.println(query);
		}
		// debug cases, exceptions
		if (result.equals("$Failed"))
			System.out.println("Failed rho intersection query ->" + query);
		if (result.contains("C[1]")) {
			// Mathematica is not working in this case
			//System.out.println("Slow intersection due to Mathematica formatting");
			//System.out.println(query);
			return "Format";
		}
		else if (!result.equals("False")) {
		
			// create SMT file
			String fileName = "SMTfile.smt2";
			File smtFile = new File(fileName);
			try {
				if (smtFile.createNewFile()) {
				    System.out.println("File created: " + smtFile.getName());
				  } else {
				    System.out.println("File already exists.");
				  }
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
				
				smtWritter.append("(assert (= rho_1 " + recursiveRhoToSMT(rho1.substring(rho1.indexOf(">") + 1), false) + "))\n");
				
			
				smtWritter.append("(assert (= rho_2 " + recursiveRhoToSMT(rho2.substring(rho2.indexOf(">") + 1), true) + "))\n");
	
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
				System.out.println("Successfully generated SMT file!");
				fileId++;
				
				// SMT file is built, only need to count number of solutions using approxmc and opensmt
				String command = "opensmt SMTfile.smt2 | grep -v '^; ' | approxmc |grep '^s mc' |awk '{print $3}'";
				Runtime.getRuntime().exec(command);	
			} catch (IOException e) {
				System.out.println("Error on generating SMT file!");
				e.printStackTrace();
			}
		}
		return result;
	}
	 */
	
	/*
	 // DEPRECATED method to print adjacency matrix given graph
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
		System.out.println("----------------");
	}
	
	// DEPRECATED method that computes correct edge weight
	private LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> addProbRhos(LinkedHashMap<GraphVertex, ArrayList<GraphEdge>> graph) {
		// go over all vertices in the graph
		for (Map.Entry<GraphVertex, ArrayList<GraphEdge>> vertex : graph.entrySet()) {
			// check which txProfile it belongs to
			int txProfile = vertex.getKey().getTxProfile();
			// generate all possible inputs according to txProfile and check if they belong to any edge
			// new order (w,d,c,i)
			if (txProfile == 2) { 
				for (int w_id = 0; w_id < VertexPhi.getScalingFactorW() - 1; w_id++) {
					for (int d_id = 0; d_id < VertexPhi.getScalingFactorD() - 1; d_id++) {
						for (int c_id = 0; c_id < VertexPhi.getScalingFactorC() - 1; c_id++) {
							for (int i_id = 0; i_id < VertexPhi.getScalingFactorI() - 1; i_id++) {
								String input = " && warehouse_id == " + w_id + " && district_id == " + d_id + 
												" && customer_id == " + c_id + " && item_id == " + i_id +
												", {warehouse_id, district_id, customer_id, item_id}, Integers";
								boolean foundIntersection = false;
								// go over all the edges for the vertex and check if the input is on that edge
								for (GraphEdge edge : vertex.getValue()) {
									// get the intersection
									String intersection = edge.getIntersection();
									// check if this input is in the intersection
									KernelLink link = MathematicaHandler.getInstance();
									System.out.println("Sent query!");
									String query = "FindInstance[(" + intersection + ")" + input + "]";
								
									String result = link.evaluateToOutputForm(query, 0);
									
									if (result.equals("{}")) {
										// no intersection, this input is not on the edge
										continue;
									}
									else {
										// interseciton detected, input is on the edge
										edge.addEdgeWeight();
										foundIntersection = true;
										break;
									}
								}
								// if intersection has been found, this input is on the edge
								if (foundIntersection) {
									continue;
								}
							}
							
						}
					}
				}
			}
		}
		return graph;
	
	}
	 */

	/*
	 * // method that checks if two rhos can intersect for any input
	private boolean checkIntersection(Pair<VertexRho, VertexPhi> rhoPhi1, Pair<VertexRho, VertexPhi> rhoPhi2) {
		// get rhos and phis as strings
		String rho1 = rhoPhi1.getKey().getRho();
		String rho2 = rhoPhi2.getKey().getRho();
		String phi1 = rhoPhi1.getValue().getPhiAsString();
		String phi2 = rhoPhi2.getValue().getPhiAsString();
		
		// rename vars for SMT query
		rho2 = variableRename(rho2);
		phi2 = variableRename(phi2);
		
		// obtain variable list for each rho
		HashSet<String> vars1 = rhoPhi1.getKey().getVariables(); // new vertex
		HashSet<String> vars2 = rhoPhi2.getKey().getVariables(); // old vertex
		
		// create SMT file
		String fileName = "SMTSplitfile.smt2";
		File smtFile = new File(fileName);
		
		try {
			smtFile.createNewFile();
		} catch (IOException e) {
			System.out.println("Error while creating SMT-SAT file!");
			e.printStackTrace();
		}
		// write to SMT file
		try {
			// reset tags for SMT vars in file
			smtTag = 0;
			
			FileWriter smtWritter = new FileWriter(fileName);
			// first line contains smt lib headers
			smtWritter.append("(set-logic QF_BV)\n");
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
			
			smtWritter.append("(assert (= rho_1 " + recursiveRhoToSMT(rho1.substring(rho1.indexOf(">") + 1), false) + "))\n");
		
			smtWritter.append("(assert (= rho_2 " + recursiveRhoToSMT(rho2.substring(rho2.indexOf(">") + 1), false) + "))\n");
			
			smtWritter.append("(assert (= phi_1 " + parsePhiIntoSmt(phi1) + "))\n");
			smtWritter.append("(assert (= phi_2 " + parsePhiIntoSmt(phi2) + "))\n");

			
			smtWritter.append("(assert (and (= rho_1 rho_2) (and phi_1 phi_2)))\n");
			
			smtWritter.append("(check-sat)\n");
			smtWritter.append("(exit)");
			// close file
			smtWritter.close();
			
			// SMT file is built, only need to count number of solutions using approxmc and opensmt
			Runtime rt = Runtime.getRuntime();
			String[] commandsSMT = {"opensmt", "SMTSplitfile.smt2"};
			Process proc = rt.exec(commandsSMT);

			BufferedReader stdInput = new BufferedReader(new 
			     InputStreamReader(proc.getInputStream()));

			BufferedReader stdError = new BufferedReader(new 
			     InputStreamReader(proc.getErrorStream()));
			
			
			// Read the output from the command
			String s = null;
			while ((s = stdInput.readLine()) != null) {
				System.out.println(s);
				if(s.equals("unsat")) {
			    	return false;
			    }
			}
			// Read any errors from the attempted command
			while ((s = stdError.readLine()) != null) {
				System.out.println("Error during openSMT phase");
			    System.out.println(s);
			}
			
			
		} catch (IOException e) {
			System.out.println("Error on generating SMT file!");
			e.printStackTrace();
		}
		
		return true;

	}

	 */
}
