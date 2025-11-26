package thesis;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import weka.classifiers.trees.J48;
import weka.core.Instance;
import weka.core.Instances;

public class RubisWorkloadGenerator {
	
	// scaling factors
	private static int users = VertexPhi.getRubisUsers();
	private static int items = VertexPhi.getRubisItems();
	private static int buy = VertexPhi.getRubisBuy();
	private static int bid = VertexPhi.getRubisBid();
	private static int comment = VertexPhi.getRubisComment();
	private static int region = VertexPhi.getRubisRegion();
	private static int category = VertexPhi.getRubisCategory();
	
	// current partition for tx
	static private long currentPart = -1;
	
	// no LOCAL transactions (single part)
	static int local = 0;
	// no REMOTE transactions (multi part)
	static int remote = 0;
	// num parts
	static long parts = Partitioner.getNoParts();
	
	// workload generator for Schism
	public void buildSchismTrace(int noTxs) {
		// number of txs generated
		int generatedTxs = 0;
		
		try {
			// create a file for Weka training
			File schism = new File("schism.txt");
			schism.createNewFile();
			FileWriter schismWriter = new FileWriter("schism.txt", false);
			// create the number of desired transactions in a trace
			while (generatedTxs < noTxs) {
				// increment genTxs
				generatedTxs++;
				// string to hold line for trace file
				String traceLine = "";
				// roll the dice to know which tx to generate
				int randomNum = ThreadLocalRandom.current().nextInt(0, 100);
				// buy now tx
				if (randomNum < 20) {
					// generate random item id
					long randItem = ThreadLocalRandom.current().nextInt(0, items * category);
					traceLine += "i" + randItem;
					// generate random number of buy nows
					long randBuy = ThreadLocalRandom.current().nextInt(0, buy);
					traceLine += " i" + randItem + "y" + randBuy;
					traceLine += "\n";
					schismWriter.append(traceLine);
				}
				// bid tx
				else if (randomNum < 40) {
					// generate random item id
					long randItem = ThreadLocalRandom.current().nextInt(0, items * category);
					traceLine += "i" + randItem;
					// generate random number of buy nows
					long randBid = ThreadLocalRandom.current().nextInt(0, bid);
					traceLine += " i" + randItem + "d" + randBid;
					traceLine += "\n";
					schismWriter.append(traceLine);
				}
				// comment tx
				else if (randomNum < 60) {
					// generate random from id
					long randFrom = ThreadLocalRandom.current().nextInt(0, users * region);
					traceLine += "u" + randFrom;
					long randTo = ThreadLocalRandom.current().nextInt(0, users * region);
					traceLine += " u" + randTo;
					long randComment = ThreadLocalRandom.current().nextInt(0, comment);
					traceLine += " u" + randFrom + "t" + randComment;
					traceLine += "\n";
					schismWriter.append(traceLine);
				}
				// register user
				else if (randomNum < 80) {
					// generate random region
					long randRegion = ThreadLocalRandom.current().nextInt(0, region);
					traceLine += "r" + randRegion;
					// generate random user
					long randUser = ThreadLocalRandom.current().nextInt(0, users * region);
					traceLine+= " r" + randRegion + "u" + randUser;
					traceLine += "\n";
					schismWriter.append(traceLine);
				}
				// register item
				else {
					// generate random category
					long randCategory = ThreadLocalRandom.current().nextInt(0, category);
					traceLine += "c" + randCategory;
					// generate random item
					long randItem = ThreadLocalRandom.current().nextInt(0, items * category);
					traceLine+= " c" + randCategory + "i" + randItem;
					traceLine += "\n";
					schismWriter.append(traceLine);
				}	
			}
			schismWriter.close();
			System.out.println("Successfully generated Schism trace file with " + generatedTxs + " txs!");
		}
		catch (IOException e) {
			System.out.println("Error on generating schism trace file!");
			e.printStackTrace();
		}
		
	}
	// method used to evaluate Schism in terms of % of distributed txs given a part logic
	public void evaluateSchismRubis(int noTxs, int noP, J48 logic) {
		// printout test parameters
		System.out.println("NO users: " + users * region);
		System.out.println("NO items: " + items * category);
		System.out.println("NO parts: " + parts);
		// number of txs generated
		int generatedTxs = 0;
		// reset counters
		local = 0;
		remote = 0;
		
		while (generatedTxs < noTxs) {
			// increment genTxs
			generatedTxs++;
			// string to hold line for trace file
			String traceLine = "";
			// roll the dice to know which tx to generate
			int randomNum = ThreadLocalRandom.current().nextInt(0, 100);
			// buy now tx
			if (randomNum < 20) {
				// generate random item id
				long randItem = ThreadLocalRandom.current().nextInt(0, items * category);
				traceLine += "i" + randItem;
				// generate random number of buy nows
				long randBuy = ThreadLocalRandom.current().nextInt(0, buy);
				traceLine += " i" + randItem + "y" + randBuy;
				traceLine += "\n";
			}
			// bid tx
			else if (randomNum < 40) {
				// generate random item id
				long randItem = ThreadLocalRandom.current().nextInt(0, items * category);
				traceLine += "i" + randItem;
				// generate random number of buy nows
				long randBid = ThreadLocalRandom.current().nextInt(0, bid);
				traceLine += " i" + randItem + "d" + randBid;
				traceLine += "\n";
			}
			// comment tx
			else if (randomNum < 60) {
				// generate random from id
				long randFrom = ThreadLocalRandom.current().nextInt(0, users * region);
				traceLine += "u" + randFrom;
				long randTo = ThreadLocalRandom.current().nextInt(0, users * region);
				traceLine += " u" + randTo;
				long randComment = ThreadLocalRandom.current().nextInt(0, comment);
				traceLine += " u" + randFrom + "t" + randComment;
				traceLine += "\n";
			}
			// register user
			else if (randomNum < 80) {
				// generate random region
				long randRegion = ThreadLocalRandom.current().nextInt(0, region);
				traceLine += "r" + randRegion;
				// generate random user
				long randUser = ThreadLocalRandom.current().nextInt(0, users * region);
				traceLine+= " r" + randRegion + "u" + randUser;
				traceLine += "\n";
			}
			// register item
			else {
				// generate random category
				long randCategory = ThreadLocalRandom.current().nextInt(0, category);
				traceLine += "c" + randCategory;
				// generate random item
				long randItem = ThreadLocalRandom.current().nextInt(0, items * category);
				traceLine+= " c" + randCategory + "i" + randItem;
				traceLine += "\n";
			}	
		}
		
		// Print out the results
		System.out.println("Number of transactions in experiment: " + noTxs);
		System.out.println("Remote transactions: " + remote);
		System.out.println("Local transactions: " + local);
		double percent =  ( (double) remote/ (double) noTxs);
		System.out.println("Percentage of remote transactions: " + (percent * 100) + "%");
	}
	

	// method used to evaluate Catalyst in terms of % of distributed txs given a part logic
	public void evaluateCatalystRubis(int noTxs, LinkedHashMap<Integer,LinkedHashMap<Split, Integer>> logic) {
		// printout test parameters
		System.out.println("NO users: " + users * region);
		System.out.println("NO items: " + items * category);
		System.out.println("NO parts: " + parts);
		// number of txs generated
		int generatedTxs = 0;
		// reset counters
		local = 0;
		remote = 0;
		//generate txs until desired number
		while (generatedTxs < noTxs) {
			// part for current tx
			currentPart = -1;
			// feature list for each
			ArrayList<Pair<String, Integer>> features;
			HashMap<String, Integer> featureMap;
			// increase no generated txs
			generatedTxs++;
			// roll the dice to know which tx to generate
			int randomNum = ThreadLocalRandom.current().nextInt(0, 100);
			// store buy txs
			if (randomNum < 20) {
				System.out.println("Generated a store buy");
				// generate a random item
				long randItem = ThreadLocalRandom.current().nextInt(0, (category * 100000) + items);
				long itemKey = randItem;
				Pair<String, Integer> itemFeature = new Pair<String, Integer>("itemId", (int) randItem);
				features = new ArrayList<>();
				features.add(itemFeature);
				featureMap = buildFeatureMap(features);
				if(!checkPart(3, itemKey, 2, featureMap, logic)) continue;
				// generate bought items
				long randBuy = ThreadLocalRandom.current().nextInt(0, buy);
				long buyKey = randItem*100 + randBuy;
				Pair<String, Integer> buyFeature = new Pair<String, Integer>("buyId", (int) randBuy);
				features = new ArrayList<>();
				features.add(itemFeature);
				features.add(buyFeature);
				featureMap = buildFeatureMap(features);
				if(!checkPart(3, buyKey, 6, featureMap, logic)) continue;
				local++;
			}
			// store bid txs
			else if (randomNum < 40) {
				System.out.println("Generated a store bid");
				// generate a random item
				long randItem = ThreadLocalRandom.current().nextInt(0, (category * 100000) + items);
				long itemKey = randItem;
				Pair<String, Integer> itemFeature = new Pair<String, Integer>("itemId", (int) randItem);
				features = new ArrayList<>();
				features.add(itemFeature);
				featureMap = buildFeatureMap(features);
				if(!checkPart(4, itemKey, 2, featureMap, logic)) continue;
				// generate bid items
				long randBid = ThreadLocalRandom.current().nextInt(0, bid);
				long bidKey = randItem*1000 + randBid;
				Pair<String, Integer> bidFeature = new Pair<String, Integer>("bidId", (int) randBid);
				features = new ArrayList<>();
				features.add(itemFeature);
				features.add(bidFeature);
				featureMap = buildFeatureMap(features);
				if(!checkPart(4, bidKey, 5, featureMap, logic)) continue;
				local++;
			}
			// store comment tx
			else if (randomNum < 60) {
				System.out.println("Generated a store comment");
				// generate a random from user
				long randFrom = ThreadLocalRandom.current().nextInt(0, (region * 100000) + users);
				long fromKey = randFrom;
				Pair<String, Integer> fromFeature = new Pair<String, Integer>("fromId", (int) randFrom);
				features = new ArrayList<>();
				features.add(fromFeature);
				featureMap = buildFeatureMap(features);
				if(!checkPart(5, fromKey, 1, featureMap, logic)) continue;
				// generate a random to user
				long randTo = ThreadLocalRandom.current().nextInt(0, (region * 100000) + users);
				long toKey = randFrom;
				Pair<String, Integer> toFeature = new Pair<String, Integer>("toId", (int) randTo);
				features = new ArrayList<>();
				features.add(toFeature);
				featureMap = buildFeatureMap(features);
				if(!checkPart(5, toKey, 1, featureMap, logic)) continue;
				// generate a random comment
				long randComment = ThreadLocalRandom.current().nextInt(0, comment);
				long commentKey = randFrom*100 + randComment;
				Pair<String, Integer> commentFeature = new Pair<String, Integer>("commentId", (int) randComment);
				features = new ArrayList<>();
				features.add(fromFeature);
				features.add(commentFeature);
				featureMap = buildFeatureMap(features);
				if(!checkPart(5, commentKey, 7, featureMap, logic)) continue;
				local++;
			}
			// register user tx
			else if (randomNum < 80) {
				System.out.println("Generated a register user");
				// generate a random region
				long randRegion = ThreadLocalRandom.current().nextInt(0, region);
				long regionKey = randRegion;
				Pair<String, Integer> regionFeature = new Pair<String, Integer>("regionId", (int) randRegion);
				features = new ArrayList<>();
				features.add(regionFeature);
				featureMap = buildFeatureMap(features);
				if(!checkPart(1, regionKey, 4, featureMap, logic)) continue;
				// generate a random user
				long randUser = ThreadLocalRandom.current().nextInt(0, users);
				long userKey = (randRegion * 100000) + randUser;
				Pair<String, Integer> userFeature = new Pair<String, Integer>("userId", (int) randUser);
				features = new ArrayList<>();
				features.add(regionFeature);
				features.add(userFeature);
				featureMap = buildFeatureMap(features);
				if(!checkPart(1, userKey, 1, featureMap, logic)) continue;
				local++;
			}
			// register item tx
			else {
				System.out.println("Generated a register item");
				// generate a random category
				long randCategory = ThreadLocalRandom.current().nextInt(0, category);
				long categoryKey = randCategory;
				Pair<String, Integer> categoryFeature = new Pair<String, Integer>("categoryId", (int) randCategory);
				features = new ArrayList<>();
				features.add(categoryFeature);
				featureMap = buildFeatureMap(features);
				if(!checkPart(2, categoryKey, 3, featureMap, logic)) continue;
				// generate a random item
				long randItem = ThreadLocalRandom.current().nextInt(0, items);
				long itemKey = randItem + (randCategory * 100000);
				Pair<String, Integer> itemFeature = new Pair<String, Integer>("itemId", (int) randItem);
				features = new ArrayList<>();
				features.add(categoryFeature);
				features.add(itemFeature);
				featureMap = buildFeatureMap(features);
				if(!checkPart(2, itemKey, 2, featureMap, logic)) continue;
				local++;
			}
		}
		// Print out the results
		System.out.println("Number of transactions in experiment: " + noTxs);
		System.out.println("Remote transactions: " + remote);
		System.out.println("Local transactions: " + local);
		double percent =  ( (double) remote/ (double) noTxs);
		System.out.println("Percentage of remote transactions: " + (percent * 100) + "%");
	}
	
	// method that returns true if an access is local or false if it is remote Schism
	public boolean checkPart(int table, Instances dataSet, Instance key, J48 logic) {
		// part the access belongs to
		int part = -1;
		
		System.out.println("Table: " + table + " Key: " + key);

		part = (int) Math.abs((int) (key.toString().hashCode()) % parts);		
		
		if (logic != null) {
			try {
				part = (int) logic.classifyInstance(key);
			} catch (Exception e) {
				System.out.println("Error on classifying instance");
				e.printStackTrace();
			}
		}
		
		System.out.println("Part: " + part);
		
		// check if local or remote
		if (currentPart == -1) {
			currentPart = part;
			return true;
		}
		else if (currentPart == part) {
			return true;
		}
		else {
			remote++;
			return false;
		}
	}
	
	
	// method that returns true if an access is local or false if it is remote
	public static boolean checkPart(int txProfile, long key, int table, HashMap<String, Integer> features, LinkedHashMap<Integer,LinkedHashMap<Split, Integer>> logic) {
		// part the access belongs to
		int part = -1;
				
		System.out.println("Table: " + table + " Key: " + key);
		
		if (table == 1) {
			if (key <= 2999999)
				part = 1;
			else
				part = 2;
		}
		
		else if (table == 2) {
			if (key <= 499999)
				part = 1;
			else
				part = 2;
		}
		
		else if (table == 3) {
			int category_id = features.get("categoryId");
			
			if (category_id <= 4)
				part = 1;
			else
				part = 2;
		}
		
		else if (table == 4) {
			int region_id = features.get("regionId");
			
			if (region_id <= 29)
				part = 1;
			else
				part = 2;
		}
		
		else {
			if (features.containsKey("itemId")) {
				int item_id = features.get("itemId");
				
				if (item_id <= 499999)
					part =1;
				else
					part = 2;
			}
			else {
				int item_id = features.get("fromId");
				
				if (item_id <= 499999)
					part =1;
				else
					part = 2;
			}	
		}
		
		/*
		if (logic != null) {
			// query the map to get the rules for correct txProfile
			LinkedHashMap<Split, Integer> rules = logic.get(txProfile);
			// given the rules just need to query them in order
			for (Map.Entry<Split, Integer> rule : rules.entrySet()) {
				if (rule.getKey().query(key, table, features))
					part = rule.getValue();
			}
		}
		
		else {
			part = (int) (key % parts);
		}*/
		

		System.out.println("Part: " + part);
		
		// check if local or remote
		if (currentPart == -1) {
			currentPart = part;
			return true;
		}
		else if (currentPart == part) {
			return true;
		}
		else {
			if (VertexPhi.checkTableReplicated(table)) {
				return true;
			}
			System.out.println("Remote");
			remote++;
			return false;
		}
	}
		
	// method that builds map of features for each access
	public HashMap<String, Integer> buildFeatureMap(ArrayList<Pair<String, Integer>> features) {
		// build a feature map
		HashMap<String, Integer> featureMap = new HashMap<>();
		// iterate over pairs of features and insert in map
		for (Pair<String, Integer> feature : features) {
			featureMap.put(feature.getKey(), feature.getValue());
		}
		return featureMap;
	}
}
