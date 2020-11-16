package thesis;

import java.util.*;
import java.io.*;

// class responsible for parsing through SE file and generating vertices
public class SEParser {
	// list of vertices obtained from parsing SE file
	private static ArrayList<Vertex> vertices = new ArrayList<>();
	
	// default constructor for parser, takes files to read as input
	public SEParser(String[] files) throws IOException {
		int txProfile = 1;
		for (String file : files) {
			FileReader input = new FileReader(file);
			BufferedReader bufRead = new BufferedReader(input);
			String line = null;
			buildVertices(bufRead, line, txProfile);
			txProfile++;
		}
	}

	// method to build vvertices by reading SE file
	private void buildVertices(BufferedReader file, String line, int txProfile) throws IOException {
		// iterator for empty branches
		int emptyBranch = 0;
		// hashmap to represent SE tree
		LinkedHashMap<String, SETreeNode> tree = new LinkedHashMap<>();
		// root must have different behavior
		boolean root = true;
		// iterate SE file
		while ((line = file.readLine()) != null) {
			// check for new nodes
			if (line.startsWith("[PC")) {
				// path condition identifier for this node
				String pc = line.substring(line.indexOf("[") + 1, line.indexOf("]"));
				// check if root
				if (root) {
					// in root first line is its RWSet tag, discard it
					line = file.readLine();
					// obtain left subtree if it exists
					line = file.readLine();
				}
				else {
					// outside of root node next line is children info
					line = file.readLine();
				}
				// check for children
				if (line.startsWith("  --> [PC")) {
					// node has children, get both path conditions
					String pcLeft = line.substring(line.indexOf("[") + 1, line.indexOf("]"));
					// second pc might be empty if no new further accesses, check if it exists
					String pcRight = "#" + emptyBranch;
					line = file.readLine();
					if (line.startsWith("  --> [PC")) {
						// there is a right children
						pcRight = line.substring(line.indexOf("[") + 1, line.indexOf("]"));
					}
					else {
						// empty branch found
						tree.put("#" + emptyBranch, new SETreeNode(null, null));
						emptyBranch++;
					}
					// create node
					SETreeNode node = new SETreeNode(pcLeft, pcRight);
					// root rwset is in the end of the file
					if (!root) {
						// read all lines until rwset
						while (!(line = file.readLine()).startsWith("[RWSET"));
						// get next line with read or write set info
						line = file.readLine();
						// check if there is a read set
						node = getRSet(line, node);
						// check if there is a write set
						line = file.readLine();
						node = getWSet(line, node);
						// add node to the tree
						tree.put(pc, node);
					}
					else {
						// disable root
						root = false;
						// add root to tree
						tree.put("root", node);
					}
					
				}
				else {
					// node has no children, read until rwSet is found
					SETreeNode node = new SETreeNode(null, null);
					while (!(line = file.readLine()).startsWith("[RWSET"));
					// get next line with read or write set info
					line = file.readLine();
					// check if there is a read set
					node = getRSet(line, node);
					// check if there is a write set
					line = file.readLine();
					node = getWSet(line, node);
					// add node to the tree, if root add tag
					if (root) {
						tree.put("root", node);
					}
					else {
						tree.put(pc, node);
					}
				}
			}
			// root read write set need sto be consumed too, only applies if root has children
			else if (line.startsWith("[RWSET")) {
				SETreeNode rootNode = tree.get("root");
				// check if root is a leaf itself
				if (!rootNode.isLeaf()) {
					// get next line with read or write set info
					line = file.readLine();
					// check if there is a read set
					rootNode = getRSet(line, rootNode);
					// check if there is a write set
					line = file.readLine();
					rootNode = getWSet(line, rootNode);
					// add node to the tree
					tree.put("root", rootNode);
				}
			}
		}
		// create array of rwets
		//ArrayList<Pair<HashSet<String>, HashSet<String>>> rwSets = new ArrayList<>();
		// traverse tree to generate all the vertices
		// traverseTree(tree, tree.get("root"), rwSets, txProfile);TODO remove, used for smart delivery
		// merge all tree leaves into a single vertex
		mergeTree(tree, txProfile);
	}
	
	// method that merges all tree leaves into a single vertex
	public void mergeTree(LinkedHashMap<String, SETreeNode> tree, int txProfile) {
		// create new Vertex
		Vertex vertex = new Vertex(txProfile);
		// go over tree
		for (Map.Entry<String, SETreeNode> leaf : tree.entrySet()) {
			// get read set
			HashSet<String> rSet = leaf.getValue().getReadSet();
			// get write set
			HashSet<String> wSet = leaf.getValue().getWriteSet();
			// add readSet if it exists
			if (rSet != null) {
				vertex.addToReadSet(rSet);
			}
			// add writeSet if it exists
			if (wSet != null) {
				vertex.addToWriteSet(wSet);
			}
		}
		
		// add vertex to vertices
		vertices.add(vertex);
	}
	
	// method that generates SE vertices from traversing tree
	public void traverseTree(LinkedHashMap<String, SETreeNode> tree, SETreeNode node, ArrayList<Pair<HashSet<String>, HashSet<String>>> rwSets, int txProfile) {
		if (node != null) {
			// get read set
			HashSet<String> rSet = node.getReadSet();
			// get write set
			HashSet<String> wSet = node.getWriteSet();
			//create read write set tuple for node
			Pair<HashSet<String>, HashSet<String>> rwSet = new Pair<HashSet<String>, HashSet<String>>(rSet, wSet);
			// add rwset to the list of rwsets for current branch
			rwSets.add(rwSet);
			// check if its a leaf
			if (node.isLeaf()) {
				// create vertex with all the read and write sets combined
				Vertex vertex = new Vertex(txProfile);
				// add read and write sets
				for (Pair<HashSet<String>, HashSet<String>> entry : rwSets) {
					// add readSet if it exists
					if (entry.getKey() != null) {
						vertex.addToReadSet(entry.getKey());
					}
					// add writeSet if it exists
					if (entry.getValue() != null) {
						vertex.addToWriteSet(entry.getValue());
					}
				}
				// vertex rwset is done, finalize vertex creation
				vertices.add(vertex);
			}
			else {
				// check right subtree
				traverseTree(tree, tree.get(node.getPcRight()), rwSets, txProfile);
				// check left subtree
				traverseTree(tree, tree.get(node.getPcLeft()), rwSets, txProfile);
			}
		}
		
	}
	
	
	// method to obtain readSet for a given line
	public SETreeNode getRSet(String line, SETreeNode node) {
		// check if there is a read set
		if (line.startsWith("R:")) {
			// there is a readSet
			line = line.substring(line.indexOf(":") + 2);
			// obtain all read formulas
			String[] readFormulas = line.split(", ");
			HashSet<String> readSet = new HashSet<>();
			for (String formula : readFormulas) {
				readSet.add(formula);
			}
			// set read set
			node.setReadSet(readSet);
		}
		return node;
	}
	
	// method to obtain writeSet for a given line
	public SETreeNode getWSet(String line, SETreeNode node) {
		// check if there is a write set
		if (line.startsWith("W:")) {
			// there is a writeset
			line = line.substring(line.indexOf(":") + 2);
			// obtain all write formulas
			String[] writeFormulas = line.split(", ");
			HashSet<String> writeSet = new HashSet<>();
			for (String formula : writeFormulas) {
				writeSet.add(formula);
			}
			// set write set
			node.setWriteSet(writeSet);
		}
		return node;
	}
	
	// getter for vertices generated from parsing SE file
	public static ArrayList<Vertex> getVertices() {
		return vertices;
	}
	
	
	// main for debug purposes
	public static void main(String[] args) {
		try {
			String[] files = {"delivery_simple.txt"};
			new SEParser(files);
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