package thesis;

import java.util.*;
import java.io.*;

public class Parser {

	public static ArrayList<Vertex> vertices = new ArrayList<>();

	public Parser(String file) throws IOException {
		FileReader input = new FileReader(file);
		BufferedReader bufRead = new BufferedReader(input);
		String line = null;
		buildVertices(bufRead, line);
	}

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
				// remove Readset tag from string
				line = line.substring(line.indexOf(":") + 2);
				// obtain all read formulas
				String[] readFormulas = line.split(", ");
				for (String formula : readFormulas) {
					vertex.addToReadSet(formula);
				}
			}
			// write set
			else if (rwsetFound && line.startsWith("W:")) {
				// remove Writeset tag from string
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

	public static void main(String[] args) {
		try {
			//Parser p = new Parser("delivery_final_.txt");
			Parser p = new Parser("payment_final.txt");
			int vertexCount = 0;
			for(Vertex vertex : vertices) {
				vertexCount++;
				System.out.println("Vertex no " + vertexCount);
				vertex.printVertex();
			}
		} catch (IOException e) {
			System.out.println("Symbolic execution file not found");
			e.printStackTrace();
		}
	}

}