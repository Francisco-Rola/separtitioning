package thesis;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

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
	public static void buildSchismTrace(int noTxs) {
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
				if (randomNum < 45) {
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
				else if (randomNum < 88) {
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
				else if (randomNum < 92) {
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
				else if (randomNum < 96) {
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
			if (randomNum < 44) {
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
			else if (randomNum < 87) {
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
			else if (randomNum < 91) {
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
			else if (randomNum < 95) {
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
	// method that returns true if an access is local or false if it is remote
	public static boolean checkPart(int txProfile, long key, int table, HashMap<String, Integer> features, LinkedHashMap<Integer,LinkedHashMap<Split, Integer>> logic) {
		// part the access belongs to
		int part = -1;
		// query the map to get the rules for correct txProfile
		LinkedHashMap<Split, Integer> rules = logic.get(txProfile);
		// given the rules just need to query them in order
		for (Map.Entry<Split, Integer> rule : rules.entrySet()) {
			if (rule.getKey().query(key, table, features))
				part = rule.getValue();
		}	
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
			System.out.println("Table: " + table + " - Remote key: " + key);
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
