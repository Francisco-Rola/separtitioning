package thesis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import com.wolfram.jlink.*;

public class GraphBuilder {
	
	private static ArrayList<GraphVertex> vertices = new ArrayList<>();
	
	private static String buildPhi(HashSet<String> phis) {
		String finalPhi = "";
		boolean isFirst = true;
		for (String phi: phis) {
			if (!isFirst) {
				finalPhi += " && ";
			}
			finalPhi += phi;
			isFirst = false;
		}
		return finalPhi;
	}
	
	private static String findVariables(String rho1, String rho2) {
		HashSet<String> variables = new HashSet<>();
		Matcher m = Pattern.compile("\\w+_id").matcher(rho1);
		while(m.find()) {
			variables.add(rho1.substring(m.start(), m.end()));
			//System.out.println(rho1.substring(m.start(), m.end()));
		}
		m = Pattern.compile("\\w+_id").matcher(rho2);
		while (m.find()) {
			variables.add(rho2.substring(m.start(), m.end()));
			//System.out.println(rho2.substring(m.start(), m.end()));
		}
		String variableList = "{";
		for (String variable: variables) {
			variableList += variable + ", ";
		}
		
		variableList = variableList.substring(0, variableList.length() - 2);
		System.out.println(variableList + "}");
		return variableList + "}";
	}
	
	private static boolean logicalAdd(Vertex v, KernelLink link) {
		// need to compare newly added vertex to every exisiting vertex
		String phiV = buildPhi(v.getPhis());
		//System.out.printf("Phi V: %s\n", phiV);
		for (GraphVertex gv: vertices) {
			String phiGV = buildPhi(gv.getSigma().getPhis());
			//System.out.printf("Phi GV: %s\n", phiGV);
			// compare rhos of v and gv that access the same table
			for (String rhoV: v.getRhos()) {
				//System.out.printf("Rho V: %s\n", rhoV);
				for (String rhoGV: gv.getSigma().getRhos()) {
					//System.out.printf("Rho GV: %s\n", rhoGV);
					// check if the rhos are related to the same table
					if (rhoV.charAt(0) == rhoGV.charAt(0)) {
						//compute intersection between rhos given the phis
						String rhoQuery = rhoV.substring(3) + " == " + rhoGV.substring(3);
						String phiQuery = phiV + " && " + phiGV;
						findVariables(rhoV, rhoGV);
						String query = "Reduce[" + rhoQuery + " && " + phiQuery + ", {warehouse_id}, Integers]";
						System.out.println(query);
						String result = link.evaluateToOutputForm(query, 0);
						System.out.println(result);
						return true;
					}
					continue;
				}
			}
		}
		return false;
	}
	
	private static void buildGraph(KernelLink link) {
		try {
			String[] files = {"payment_final.txt", "new_order_final.txt"};
			Parser p = new Parser(files);
			
			// After obtaining vertices from SE need to make them disjoint
			ArrayList<Vertex> seVertices = Parser.getVertices();
			
			boolean isFirst = true;
			
			for (Vertex v: seVertices) {
				if (isFirst) {
					// first vertex doesn't need subtracting
					isFirst = false;
					VertexSigma sigma = new VertexSigma(v.getPhis(), v.getRhos());
					GraphVertex gv = new GraphVertex(sigma);
					vertices.add(gv);
					System.out.println("First vertex added successfully");
				}
				logicalAdd(v, link);
			}
		} catch (IOException e1) {
			System.out.println("Unable to parse through SE files");
			e1.printStackTrace();
		}
	}

	public static void main(String[] args) {
		
		System.out.println("Running graph builder");
		
		KernelLink link = MathematicaHandler.getInstance();
		
		buildGraph(link);

	}
}