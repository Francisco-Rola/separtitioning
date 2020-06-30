package thesis;

import java.io.IOException;
import java.util.ArrayList;

import com.wolfram.jlink.*;

public class GraphBuilder {
	
	private static ArrayList<GraphVertex> vertices = new ArrayList<>();
	
	private boolean logicalAdd(Vertex v) {
		// need to compare newly added vertex to every exisiting vertex
		for (GraphVertex gv: vertices) {
			// compare rhos of v and gv that access the same table
			for (String rhoV: v.getRhos()) {
				for (String rhoGV: gv.getSigma().getRhos()) {
					// check if the rhos are related to the same table
					if (rhoV.charAt(0) == rhoGV.charAt(0)) {
						//compute intersection between rhos given the phis
						
					}
					continue;
				}
			}
		}
		return false;
	}

	public static void main(String[] args) {
		
		System.out.println("Running graph builder");
		
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
				// need to perform logical subtraction to avoid overlapping vertices
				
				
			}
			
			
			
			
			
		} catch (IOException e1) {
			System.out.println("Unable to parse through SE files");
			e1.printStackTrace();
		}
		
		
		KernelLink ml = null;
        try {
        	String jLinkDir = "/usr/local/Wolfram/Mathematica/12.1/SystemFiles/Links/JLink";
            System.setProperty("com.wolfram.jlink.libdir", jLinkDir);
            ml = MathLinkFactory.createKernelLink("-linkmode launch -linkname 'math -mathlink'");
        } catch (MathLinkException e) {
            System.out.println("Fatal error opening link: " + e.getMessage());
            return;
        }

        try {
            // Get rid of the initial InputNamePacket the kernel will send
            // when it is launched.
            ml.discardAnswer();
            
            System.out.println("Mathematica operations concluded!");
          
         
            
            

        } catch (MathLinkException e) {
            System.out.println("MathLinkException occurred: " + e.getMessage());
        } finally {
            ml.close();
        }

	}
}