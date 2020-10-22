package thesis;

import java.util.*;
import java.io.*;

// class responsible for parsing through SE file and generating vertices
public class Parser {
	// list of vertices obtained from parsing SE file
	private static ArrayList<Vertex> vertices = new ArrayList<>();
	
	// default constructor for parser, takes files to read as input
	public Parser(String[] files) throws IOException {
		for (String file : files) {
			FileReader input = new FileReader(file);
			BufferedReader bufRead = new BufferedReader(input);
			String line = null;
			buildVertices(bufRead, line);
		}
	}

	// method to build vvertices by reading SE file
	private void buildVertices(BufferedReader file, String line) throws IOException {
		Boolean rwsetFound = false;
		Vertex vertex = new Vertex();
		// read through the whole file
		while ((line = file.readLine()) != null) {
			// found a write read write set for a symbolic execution leaf
			if (line.startsWith("[RWSET") && rwsetFound == false) {
				rwsetFound = true;
				vertex = new Vertex();
				continue;
			}
			// read set
			if (rwsetFound && line.startsWith("R:")) {
				// remove readset tag from string
				line = line.substring(line.indexOf(":") + 2);
				// obtain all read formulas
				String[] readFormulas = line.split(", ");
				for (String formula : readFormulas) {
					vertex.addToReadSet(formula);
				}
			}
			// write set
			else if (rwsetFound && line.startsWith("W:")) {
				// remove writeset tag from string
				line = line.substring(line.indexOf(":") + 2);
				// obtain all write formulas
				String[] writeFormulas = line.split(", ");
				for (String formula : writeFormulas) {
					vertex.addToWriteSet(formula);
				}
			}
			// no read or write set left
			else if (rwsetFound) {
				// add current vertex to vertices 
				vertices.add(vertex);
				if (line.startsWith("[RWSET")) {
					continue;
				}
				else {
					rwsetFound = false;
				}
			}

		}
	}
	
	// getter for vertices generated from parsing SE file
	public static ArrayList<Vertex> getVertices() {
		return vertices;
	}
	
	// main for debug purposes
	public static void main(String[] args) {
		try {
			String[] files = {"payment_final.txt", "new_order_final.txt"};
			Parser p = new Parser(files);
			int vertexCount = 0;
			for(Vertex vertex : vertices) {
				vertexCount++;
				System.out.println("Vertex no " + vertexCount);
				vertex.printVertex();
			}
			System.out.println("Number of symbolic vertices: " + vertexCount);
		} catch (IOException e) {
			System.out.println("Symbolic execution file not found");
			e.printStackTrace();
		}
	}

}