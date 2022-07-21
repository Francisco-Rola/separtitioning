package thesis;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import weka.classifiers.trees.J48;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

public class TPCCWorkloadGenerator {
	
	// scaling factors
	private static int w;
	private static int d = VertexPhi.getScalingFactorD();
	private static int id = VertexPhi.getScalingFactorI();
	private static int c = VertexPhi.getScalingFactorC();
	
	// current partition for tx
	static private long currentPart = -1;
	
	// no LOCAL transactions (single part)
	static int local = 0;
	// no REMOTE transactions (multi part)
	static int remote = 0;
	// num parts
	static long parts = Partitioner.getNoParts();
	
	// constructor that receives scaling parameters
	public TPCCWorkloadGenerator(int w) {
		TPCCWorkloadGenerator.w = w + 1;
	}
	
	// workload generator for Schism
	public void buildSchismTrace(int noTxs, int noW) {
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
				// new order txs
				if (randomNum < 44) {
					// generate random warehouse 1
					long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
					// generate supply warehouse 1% remote
					long supplyW = randW;
					long randSupplyW = ThreadLocalRandom.current().nextInt(0, 100);
					// generate 1% remote new orders
					if (w > 1 && randSupplyW == 50) {
						while (supplyW == randW)
							supplyW = ThreadLocalRandom.current().nextInt(0, w);
					}
					traceLine += "w" + randW;
					// generate random district 2
					long randD = ThreadLocalRandom.current().nextInt(0, d);
					traceLine += " w" + randW + "d" + randD;
					// generate random customer 3
					long randC = ThreadLocalRandom.current().nextInt(0, c);
					traceLine += " w" + randW + "d" + randD + "c" + randC;
					// get order number and increment counter 5 6
					long randO = ThreadLocalRandom.current().nextInt(0, c);
					traceLine += " w" + randW + "d" + randD + "o" + randO;
					traceLine += " w" + randW + "d" + randD + "no" + randO;
					// generate number of items in order, between 5 and 15
					long randNoItems = ThreadLocalRandom.current().nextInt(4, 15);
					// iterate over the the order items
					HashSet<Long> generatedItems = new HashSet<>();
					for (int i = 0; i < randNoItems; i++) {
						// generate a random item 8
						long randItem = ThreadLocalRandom.current().nextInt(0, id);
						while (generatedItems.contains(randItem)) {
							randItem = ThreadLocalRandom.current().nextInt(0, id);
						}
						generatedItems.add(randItem);
						//traceLine += " i" + randItem;
						// generate a order line warehouse key 9
						traceLine += " w" + supplyW + "i" + randItem;
						// generate order line key 7
						traceLine += " w" + randW + "d" + randD + "o" + randO + "l" + i;; 
					}
					traceLine += "\n";
					schismWriter.append(traceLine);
				}
				// payment txs
				else if (randomNum < 87) {
					// 85% of payments the customer belongs to local warehouse
					int localCustomer = ThreadLocalRandom.current().nextInt(0, 100);
					if (localCustomer <= 84) {
						// generate random warehouse 1
						int randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
						traceLine += "w" + randW;
						// generate random district 2
						int randD = ThreadLocalRandom.current().nextInt(0, d);
						traceLine += " w" + randW + "d" + randD;
						// generate random customer 3
						int randC = ThreadLocalRandom.current().nextInt(0, c);
						traceLine += " w" + randW + "d" + randD + "c" + randC;
						// generate history 4
						traceLine += " w" + randW + "d" + randD + "h" + randC;
					}
					else {
						// generate random warehouse 1.1
						int randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
						// generate random warehouse 1.2, different from randW
						int randW2 = randW;
						if (w > 1) {
							while (randW == randW2) {
								randW2 = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
							}
						}
						traceLine += "w" + randW;
						// generate random district 2
						int randD = ThreadLocalRandom.current().nextInt(0, d);
						traceLine += " w" + randW + "d" + randD;
						// generate random customer 3, on a potentially remote warehouse
						int randC = ThreadLocalRandom.current().nextInt(0, c);
						traceLine += " w" + randW2 + "d" + randD + "c" + randC;
						// generate history 4, on a potentially remote warehouse
						traceLine += " w" + randW2 + "d" + randD + "h" + randC;
					}
					traceLine += "\n";
					schismWriter.append(traceLine);
				}
				// delivery txs
				else if (randomNum < 91) {
					// generate random warehouse 1
					long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
					traceLine += "w" + randW;
					// iterate over all districts
					for (long i = 0; i < d; i++) {
						// generate random customer 3
						long randC = ThreadLocalRandom.current().nextInt(0, c);
						traceLine += " w" + randW + "d" + i + "c" + randC;						
						// generate random order
						long order = ThreadLocalRandom.current().nextInt(0, c);
						traceLine += " w" + randW + "d" + i + "o" + order;
						traceLine += " w" + randW + "d" + i + "no" + order;
						// generate number of items in order, between 5 and 15
						long randNoItems = ThreadLocalRandom.current().nextInt(4, 15);
						// iterate over the the order items
						for (long j = 0; j < randNoItems; j++) {
							// generate order line key 7
							traceLine += " w" + randW + "d" + i + "o" + order + "l" + j; 
						}
					}
					traceLine += "\n";
					schismWriter.append(traceLine);
				}
				// order status tx
				else if (randomNum < 95) {
					// generate random warehouse 1
					long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
					// generate random district 2
					long randD = ThreadLocalRandom.current().nextInt(0, d);
					// generate random customer 3
					long randC = ThreadLocalRandom.current().nextInt(0, c);
					traceLine += "w" + randW + "d" + randD + "c" + randC;						
					// access order 6
					traceLine += " w" + randW + "d" + randD + "o" + randC;						
					// generate number of items in order, between 5 and 15
					long randNoItems = ThreadLocalRandom.current().nextInt(4, 15);
					for (int i = 0; i < randNoItems; i++) {
						// generate order line key 7
						traceLine += " w" + randW + "d" + randD + "o" + randC + "l" + i;; 
					}
					traceLine += "\n";
					schismWriter.append(traceLine);
				}
				// stock level tx
				else {
					// store generated orders to avoid duplicates
					HashSet <Long> generatedOrders = new HashSet<>();
					HashSet <Long> generatedItems = new HashSet<>();
					// generate random warehouse 1
					long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
					// generate random district 2
					long randD = ThreadLocalRandom.current().nextInt(0, d);
					traceLine += "w" + randW + "d" + randD;				
					for (long i = 0; i < 20; i++) {
						// generate random order
						long randO = ThreadLocalRandom.current().nextInt(0, c);
						while (generatedOrders.contains(randO)) {
							randO = ThreadLocalRandom.current().nextInt(0, c);
						}
						generatedOrders.add(randO);
						long randNoItems = ThreadLocalRandom.current().nextInt(4, 15);
						for (long j = 0; j < randNoItems; j++) {
							// generate order line key 7
							traceLine += " w" + randW + "d" + randD + "o" + randO + "l" + j;; 
							// generate a random item 8  
							long randItem = ThreadLocalRandom.current().nextInt(0, id);
							while (generatedItems.contains(randItem)) {
								randItem = ThreadLocalRandom.current().nextInt(0, id);
							}
							generatedItems.add(randItem);
							// generate a order line warehouse key 9
							traceLine += " w" + randW + "i" + randItem;
						}
					}
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
	public void evaluateSchismTPCC(int noTxs, int noW, int noP, J48 logic) {
		// printout test parameters
		System.out.println("NO warehouses: " + noW);
		System.out.println("NO parts: " + noP);
		// number of txs generated
		int generatedTxs = 0;
		// reset counters
		local = 0;
		remote = 0;
		
		// create the number of desired transactions in a trace
		while (generatedTxs < noTxs) {
			// declare weka attributes	
			Instance key;
            ArrayList<Attribute> attributeList = new ArrayList<Attribute>(2);
			Attribute wAtt = new Attribute("w");
			Attribute dAtt = new Attribute("d");
			Attribute cAtt = new Attribute("c");
            ArrayList<String> classVal = new ArrayList<String>();
            for (int i = 0; i < noP; i++) {
            	classVal.add(String.valueOf(i));
            }
			attributeList.add(wAtt);
			attributeList.add(dAtt);
			attributeList.add(cAtt);
			attributeList.add(new Attribute("@@class@@", classVal));
            Instances data = new Instances("TestInstances",attributeList,0);
            
            // part for current tx
         	currentPart = -1;	
			// increment genTxs
			generatedTxs++;
			// reset stop flag
			boolean stop = false;
			// string to hold line for trace file
			String traceLine = "";
			// roll the dice to know which tx to generate
			int randomNum = ThreadLocalRandom.current().nextInt(0, 100);
			// new order txs
			if (randomNum < 44) {
				// generate random warehouse 1
				long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
				// generate supply warehouse 1% remote
				long supplyW = randW;
				long randSupplyW = ThreadLocalRandom.current().nextInt(0, 100);
				// generate 1% remote new orders
				if (w > 1 && randSupplyW == 50) {
					while (supplyW == randW)
						supplyW = ThreadLocalRandom.current().nextInt(0, w);
				}
				key = new DenseInstance(data.numAttributes());
				data.add(key);
				key.setValue(wAtt, randW);
				key.setMissing(dAtt);
				key.setMissing(cAtt);
				data.setClassIndex(data.numAttributes() - 1);
				key.setDataset(data);
				if (!checkPart(1, data, key, logic)) continue;
				data.remove(0);
				// generate random district 2
				long randD = ThreadLocalRandom.current().nextInt(0, d);
				key = new DenseInstance(data.numAttributes());
				data.add(key);
				key.setValue(wAtt, randW);
				key.setValue(dAtt, randD);
				key.setMissing(cAtt);
				data.setClassIndex(data.numAttributes() - 1);
				key.setDataset(data);
				if (!checkPart(2, data, key, logic)) continue;
				data.remove(0);
				// generate random customer 3
				long randC = ThreadLocalRandom.current().nextInt(0, c);
				key = new DenseInstance(data.numAttributes());
				data.add(key);
				key.setValue(wAtt, randW);
				key.setValue(dAtt, randD);
				key.setValue(cAtt, randC);
				data.setClassIndex(data.numAttributes() - 1);
				key.setDataset(data);
				if (!checkPart(3, data, key, logic)) continue;
				data.remove(0);
				// get order number and increment counter 5 6
				long randO = ThreadLocalRandom.current().nextInt(0, c);
				// generate number of items in order, between 5 and 15
				long randNoItems = ThreadLocalRandom.current().nextInt(4, 15);
				// iterate over the the order items
				HashSet<Long> generatedItems = new HashSet<>();
				for (int i = 0; i < randNoItems; i++) {
					// generate a random item 8
					long randItem = ThreadLocalRandom.current().nextInt(0, id);
					while (generatedItems.contains(randItem)) {
						randItem = ThreadLocalRandom.current().nextInt(0, id);
					}
					generatedItems.add(randItem);
					//traceLine += " i" + randItem;
					// generate a order line warehouse key 9
					key = new DenseInstance(data.numAttributes());
					data.add(key);
					key.setValue(wAtt, supplyW);
					key.setMissing(dAtt);
					key.setMissing(cAtt);
					data.setClassIndex(data.numAttributes() - 1);
					key.setDataset(data);
					if (!checkPart(9, data, key, logic)) {
						stop = true;
						break;
					}
					data.remove(0);

					// generate order line key 7
					key = new DenseInstance(data.numAttributes());
					data.add(key);
					key.setValue(wAtt, randW);
					key.setValue(dAtt, randD);
					key.setMissing(cAtt);
					data.setClassIndex(data.numAttributes() - 1);
					key.setDataset(data);
					if (!checkPart(7, data, key, logic)) {
						stop = true;
						break;
					}
					data.remove(0);

				}
				
				if (!stop) local++;
				
			}
			// payment txs
			else if (randomNum < 87) {
				// 85% of payments the customer belongs to local warehouse
				int localCustomer = ThreadLocalRandom.current().nextInt(0, 100);
				if (localCustomer <= 84) {
					// generate random warehouse 1
					int randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
					traceLine += "w" + randW;
					// generate random district 2
					int randD = ThreadLocalRandom.current().nextInt(0, d);
					traceLine += " w" + randW + "d" + randD;
					// generate random customer 3
					int randC = ThreadLocalRandom.current().nextInt(0, c);
					traceLine += " w" + randW + "d" + randD + "c" + randC;
					// generate history 4
					traceLine += " w" + randW + "d" + randD + "h" + randC;
				}
				else {
					// generate random warehouse 1.1
					int randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
					// generate random warehouse 1.2, different from randW
					int randW2 = randW;
					if (w > 1) {
						while (randW == randW2) {
							randW2 = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
						}
					}
					traceLine += "w" + randW;
					// generate random district 2
					int randD = ThreadLocalRandom.current().nextInt(0, d);
					traceLine += " w" + randW + "d" + randD;
					// generate random customer 3, on a potentially remote warehouse
					int randC = ThreadLocalRandom.current().nextInt(0, c);
					traceLine += " w" + randW2 + "d" + randD + "c" + randC;
					// generate history 4, on a potentially remote warehouse
					traceLine += " w" + randW2 + "d" + randD + "h" + randC;
				}
				traceLine += "\n";
			}
			// delivery txs
			else if (randomNum < 91) {
				// generate random warehouse 1
				long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
				traceLine += "w" + randW;
				// iterate over all districts
				for (long i = 0; i < d; i++) {
					// generate random customer 3
					long randC = ThreadLocalRandom.current().nextInt(0, c);
					traceLine += " w" + randW + "d" + i + "c" + randC;						
					// generate random order
					long order = ThreadLocalRandom.current().nextInt(0, c);
					traceLine += " w" + randW + "d" + i + "o" + order;
					traceLine += " w" + randW + "d" + i + "no" + order;
					// generate number of items in order, between 5 and 15
					long randNoItems = ThreadLocalRandom.current().nextInt(4, 15);
					// iterate over the the order items
					for (long j = 0; j < randNoItems; j++) {
						// generate order line key 7
						traceLine += " w" + randW + "d" + i + "o" + order + "l" + j; 
					}
				}
				traceLine += "\n";
			}
			// order status tx
			else if (randomNum < 95) {
				// generate random warehouse 1
				long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
				// generate random district 2
				long randD = ThreadLocalRandom.current().nextInt(0, d);
				// generate random customer 3
				long randC = ThreadLocalRandom.current().nextInt(0, c);
				traceLine += "w" + randW + "d" + randD + "c" + randC;						
				// access order 6
				traceLine += " w" + randW + "d" + randD + "o" + randC;						
				// generate number of items in order, between 5 and 15
				long randNoItems = ThreadLocalRandom.current().nextInt(4, 15);
				for (int i = 0; i < randNoItems; i++) {
					// generate order line key 7
					traceLine += " w" + randW + "d" + randD + "o" + randC + "l" + i;; 
				}
				traceLine += "\n";
			}
			// stock level tx
			else {
				// store generated orders to avoid duplicates
				HashSet <Long> generatedOrders = new HashSet<>();
				HashSet <Long> generatedItems = new HashSet<>();
				// generate random warehouse 1
				long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
				// generate random district 2
				long randD = ThreadLocalRandom.current().nextInt(0, d);
				traceLine += "w" + randW + "d" + randD;				
				for (long i = 0; i < 20; i++) {
					// generate random order
					long randO = ThreadLocalRandom.current().nextInt(0, c);
					while (generatedOrders.contains(randO)) {
						randO = ThreadLocalRandom.current().nextInt(0, c);
					}
					generatedOrders.add(randO);
					long randNoItems = ThreadLocalRandom.current().nextInt(4, 15);
					for (long j = 0; j < randNoItems; j++) {
						// generate order line key 7
						traceLine += " w" + randW + "d" + randD + "o" + randO + "l" + j;; 
						// generate a random item 8  
						long randItem = ThreadLocalRandom.current().nextInt(0, id);
						while (generatedItems.contains(randItem)) {
							randItem = ThreadLocalRandom.current().nextInt(0, id);
						}
						generatedItems.add(randItem);
						// generate a order line warehouse key 9
						traceLine += " w" + randW + "i" + randItem;
					}
				}
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
	public void evaluateCatalystTPCC(int noTxs, LinkedHashMap<Integer,LinkedHashMap<Split, Integer>> logic) {
		// printout test parameters
		System.out.println("NO warehouses: " + w);
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
			// boolean variable to stop if remote tx is found
			boolean stop = false;
			// roll the dice to know which tx to generate
			int randomNum = ThreadLocalRandom.current().nextInt(0, 100);
			// new order txs
			if (randomNum < 44) {
				long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
				long randD = ThreadLocalRandom.current().nextInt(0, d);
				long randC = ThreadLocalRandom.current().nextInt(0, c);
				// generate random warehouse 1
				long warehouseKey = randW;
				Pair<String, Integer> warehouseFeature = new Pair<String, Integer>("warehouseid", (int) randW);
				features = new ArrayList<>();
				features.add(warehouseFeature);
				featureMap = buildFeatureMap(features);
				if(!checkPart(2, warehouseKey, 1, featureMap, logic)) continue;
				// initially supply warehouse is the same
				long supplyW = warehouseKey;
				// 1% of new orders should have remote supply warehouse
				int randSupplyW = ThreadLocalRandom.current().nextInt(0, 100);
				// generate 1% remote new orders
				if (w > 1 && randSupplyW == 50) {
					while (supplyW == randW)
						supplyW = ThreadLocalRandom.current().nextInt(0, w);
				}
				// generate random district 2
				long districtKey = randD + (randW * 100);
				Pair<String, Integer> districtFeature = new Pair<String, Integer>("districtid", (int) randD);
				features = new ArrayList<>();
				features.add(warehouseFeature);
				features.add(districtFeature);
				featureMap = buildFeatureMap(features);
				if(!checkPart(2, districtKey, 2, featureMap, logic)) continue;
				// generate random customer 3
				long customerKey = randD + (randW * 100) + (randC * 10000);
				Pair<String, Integer> customerFeature = new Pair<String, Integer>("customerid", (int) randC);
				features = new ArrayList<>();
				features.add(warehouseFeature);
				features.add(districtFeature);
				features.add(customerFeature);
				featureMap = buildFeatureMap(features);
				if(!checkPart(2, customerKey, 3, featureMap, logic)) continue;
				// generate random order 5
				long randO = ThreadLocalRandom.current().nextInt(0, c);
				long orderKey = randD + (randW * 100) + (randO * 10000);
				Pair<String, Integer> orderFeature = new Pair<String, Integer>("orderid", (int) randC);
				features = new ArrayList<>();
				features.add(warehouseFeature);
				features.add(districtFeature);
				features.add(orderFeature);
				featureMap = buildFeatureMap(features);
				if(!checkPart(2, orderKey, 5, featureMap, logic)) continue;
				// generate random new order 6
				long newOrderKey = randD + (randW * 100) + (randO * 10000);
				if(!checkPart(2, newOrderKey, 6, featureMap, logic)) continue;
				// generate number of items in order, between 5 and 15
				long randNoItems = ThreadLocalRandom.current().nextInt(4, 15);
				// iterate over the the order items
				for (int i = 0; i < randNoItems; i++) {
					// generate a random item 8
					long randItem = ThreadLocalRandom.current().nextInt(0, id);
					long itemKey = randItem;
					Pair<String, Integer> itemFeature = new Pair<String, Integer>("oliid", (int) randItem);
					features = new ArrayList<>();
					features.add(itemFeature);
					featureMap = buildFeatureMap(features);
					if(!checkPart(2, itemKey, 8, featureMap, logic)) {
						stop = true;
						break;
					}
					// generate a order line warehouse key 9
					long orderLineWKey = supplyW + (randItem * 100);
					Pair<String, Integer> supplyWFeature = new Pair<String, Integer>("warehouseid", (int) supplyW);
					features = new ArrayList<>();
					features.add(supplyWFeature);
					features.add(itemFeature);
					featureMap = buildFeatureMap(features);
					if(!checkPart(2, orderLineWKey, 9, featureMap, logic)) {
						stop = true;
						break;
					}
					// generate order line key 7
					long orderLineKey = Long.valueOf(randD + (randW * 100) + (randO * 1000000) + (i * 10000));
					features = new ArrayList<>();
					features.add(warehouseFeature);
					features.add(districtFeature);
					features.add(orderFeature);
					featureMap = buildFeatureMap(features);
					if(!checkPart(2, orderLineKey, 7, featureMap, logic)) {
						stop = true;
						break;
					}
				}
				if (!stop)
					local++;
			}
			// payment txs
			else if (randomNum < 87) {
				// 85% of payments the customer belongs to local warehouse
				long localCustomer = ThreadLocalRandom.current().nextInt(0, 100);
				if (localCustomer <= 84) {
					long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
					long randD = ThreadLocalRandom.current().nextInt(0, d);
					long randC = ThreadLocalRandom.current().nextInt(0, c);
					// generate random warehouse 1
					long warehouseKey = randW;
					Pair<String, Integer> warehouseFeature = new Pair<String, Integer>("warehouseid", (int) randW);
					features = new ArrayList<>();
					features.add(warehouseFeature);
					featureMap = buildFeatureMap(features);
					if(!checkPart(1, warehouseKey, 1, featureMap, logic)) continue;
					// generate random district 2
					long districtKey = randD + (randW * 100);
					Pair<String, Integer> districtFeature = new Pair<String, Integer>("districtid", (int) randD);
					features = new ArrayList<>();
					features.add(warehouseFeature);
					features.add(districtFeature);
					featureMap = buildFeatureMap(features);
					if(!checkPart(1, districtKey, 2, featureMap, logic)) continue;
					// generate random customer 3
					long customerKey = randD + (randW * 100) + (randC * 10000);
					Pair<String, Integer> customerFeature = new Pair<String, Integer>("customerid", (int) randC);
					features = new ArrayList<>();
					features.add(warehouseFeature);
					features.add(districtFeature);
					features.add(customerFeature);
					featureMap = buildFeatureMap(features);
					if(!checkPart(1, customerKey, 3, featureMap, logic)) continue;
					// generate history 4
					long historyKey = randD + (randW * 100) + (randC * 10000);
					features = new ArrayList<>();
					features.add(warehouseFeature);
					features.add(districtFeature);
					features.add(customerFeature);
					featureMap = buildFeatureMap(features);
					if(!checkPart(1, historyKey, 4, featureMap, logic)) continue;
					local++;
				}
				else {
					// generate random warehouse 1.1
					long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
					long randD = ThreadLocalRandom.current().nextInt(0, d);
					long randC = ThreadLocalRandom.current().nextInt(0, c);
					// generate random warehouse 1.2, different from randW
					long randW2 = randW;
					if (w > 1) {
						while (randW == randW2) {
							randW2 = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
						}
					}
					long warehouseKey = randW;
					Pair<String, Integer> warehouseFeature = new Pair<String, Integer>("warehouseid", (int) randW);
					features = new ArrayList<>();
					features.add(warehouseFeature);
					featureMap = buildFeatureMap(features);
					if(!checkPart(1, warehouseKey, 1, featureMap, logic)) continue;
					// generate random district 2
					long districtKey = randD + (randW * 100);
					Pair<String, Integer> districtFeature = new Pair<String, Integer>("districtid", (int) randD);
					features = new ArrayList<>();
					features.add(warehouseFeature);
					features.add(districtFeature);
					featureMap = buildFeatureMap(features);
					if(!checkPart(1, districtKey, 2, featureMap, logic)) continue;
					// generate random customer 3, on a potentially remote warehouse
					long customerKey = randD + (randW2 * 100) + (randC * 10000);
					warehouseFeature = new Pair<String, Integer>("warehouseid", (int) randW2);
					Pair<String, Integer> customerFeature = new Pair<String, Integer>("customerid", (int) randC);
					features = new ArrayList<>();
					features.add(warehouseFeature);
					features.add(districtFeature);
					features.add(customerFeature);
					featureMap = buildFeatureMap(features);
					if(!checkPart(1, customerKey, 3, featureMap, logic)) continue;
					// generate history 4, on a potentially remote warehouse
					long historyKey = randD + (randW2 * 100) + (randC * 10000);
					features = new ArrayList<>();
					features.add(warehouseFeature);
					features.add(districtFeature);
					features.add(customerFeature);
					featureMap = buildFeatureMap(features);
					if(!checkPart(1, historyKey, 4, featureMap, logic)) continue;
					local++;
				}
			}
			// delivery txs
			else if (randomNum < 91) {
				// generate random warehouse 1
				long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
				Pair<String, Integer> warehouseFeature = new Pair<String, Integer>("warehouseid", (int) randW);
				// iterate over all districts
				for (long i = 0; i < d; i++) {
					// generate random customer 3
					long randC = ThreadLocalRandom.current().nextInt(0, c);
					long customerKey = i + (randW * 100) + (randC * 10000);
					Pair<String, Integer> customerFeature = new Pair<String, Integer>("customerid", (int) randC);
					features = new ArrayList<>();
					features.add(warehouseFeature);
					features.add(customerFeature);
					featureMap = buildFeatureMap(features);
					if(!checkPart(3, customerKey, 3, featureMap, logic)) {
						stop = true;
						break;
					}
					// generate random order
					long order = ThreadLocalRandom.current().nextInt(0, c);
					long orderKey = i + (randW * 100) + (order * 10000);
					Pair<String, Integer> orderFeature = new Pair<String, Integer>("orderid", (int) order);
					features = new ArrayList<>();
					features.add(warehouseFeature);
					features.add(orderFeature);
					featureMap = buildFeatureMap(features);
					if(!checkPart(3, orderKey, 5, featureMap, logic)) {
						stop = true;
						break;
					}
					long newOrderKey = i + (randW * 100) + (order * 10000);
					features = new ArrayList<>();
					features.add(warehouseFeature);
					features.add(orderFeature);
					featureMap = buildFeatureMap(features);
					if(!checkPart(3, orderKey, 6, featureMap, logic)) {
						stop = true;
						break;
					}
					// generate number of items in order, between 5 and 15
					long randNoItems = ThreadLocalRandom.current().nextInt(4, 15);
					// iterate over the the order items
					for (long j = 0; j < randNoItems; j++) {
						// generate order line key 7
						long orderLineKey = Long.valueOf(i + (randW * 100) + (order * 1000000) + (j * 10000));
						features = new ArrayList<>();
						features.add(warehouseFeature);
						features.add(orderFeature);
						featureMap = buildFeatureMap(features);
						if(!checkPart(3, orderLineKey, 7, featureMap, logic)) {
							stop = true;
							break;
						}
					}
					if (stop) break;
				}
				if (!stop)
					local++;
			}
			// order status tx
			else if (randomNum < 95) {
				// generate random warehouse 1
				long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
				Pair<String, Integer> warehouseFeature = new Pair<String, Integer>("warehouseid", (int) randW);
				// generate random district 2
				long randD = ThreadLocalRandom.current().nextInt(0, d);
				Pair<String, Integer> districtFeature = new Pair<String, Integer>("districtid", (int) randD);
				// generate random customer 3
				long randC = ThreadLocalRandom.current().nextInt(0, c);
				Pair<String, Integer> customerFeature = new Pair<String, Integer>("districtid", (int) randC);
				long customerKey = randD + (randW * 100) + (randC * 10000);
				features = new ArrayList<>();
				features.add(warehouseFeature);
				features.add(districtFeature);
				features.add(customerFeature);
				featureMap = buildFeatureMap(features);
				if(!checkPart(4, customerKey, 3, featureMap, logic)) continue;
				// access order 6
				long orderKey = randD + (randW * 100) + (randC * 10000);
				features = new ArrayList<>();
				features.add(warehouseFeature);
				features.add(districtFeature);
				features.add(customerFeature);
				featureMap = buildFeatureMap(features);
				if(!checkPart(4, orderKey, 5, featureMap, logic)) continue;
				// generate number of items in order, between 5 and 15
				long randNoItems = ThreadLocalRandom.current().nextInt(4, 15);
				for (int i = 0; i < randNoItems; i++) {
					// generate order line key 7
					long orderLineKey = Long.valueOf(randD + (randW * 100) + (randC * 1000000) + (i * 10000));
					features = new ArrayList<>();
					features.add(warehouseFeature);
					features.add(districtFeature);
					features.add(customerFeature);
					featureMap = buildFeatureMap(features);
					if(!checkPart(4, orderLineKey, 7, featureMap, logic)) {
						break;
					}
				}
				local++;
			}
			// stock level tx
			else {
				// generate random warehouse 1
				long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
				Pair<String, Integer> warehouseFeature = new Pair<String, Integer>("warehouseid", (int) randW);
				// generate random district 2
				long randD = ThreadLocalRandom.current().nextInt(0, d);
				Pair<String, Integer> districtFeature = new Pair<String, Integer>("districtid", (int) randD);
				long districtKey = randD + (randW * 100);
				features = new ArrayList<>();
				features.add(warehouseFeature);
				features.add(districtFeature);
				featureMap = buildFeatureMap(features);
				if(!checkPart(5, districtKey, 2, featureMap, logic)) continue;
				for (long i = 0; i < 20; i++) {
					// generate random order
					long randO = ThreadLocalRandom.current().nextInt(0, c);
					Pair<String, Integer> orderFeature = new Pair<String, Integer>("orderid", (int) randO);
					long randNoItems = ThreadLocalRandom.current().nextInt(5, 15);
					for (long j = 0; j < randNoItems; j++) {
						// generate order line key 7
						long orderLineKey = Long.valueOf(randD + (randW * 100) + (randO * 1000000) + (j * 10000));
						features = new ArrayList<>();
						features.add(warehouseFeature);
						features.add(districtFeature);
						features.add(orderFeature);
						featureMap = buildFeatureMap(features);
						if(!checkPart(5, orderLineKey, 7, featureMap, logic)) {
							stop = true;
							break;
						}
						// generate a random item 8
						long randItem = ThreadLocalRandom.current().nextInt(0, id);
						Pair<String, Integer> itemFeature = new Pair<String, Integer>("oliid", (int) randItem);
						// generate a order line warehouse key 9
						long orderLineWKey = randW + (randItem * 100);
						features = new ArrayList<>();
						features.add(warehouseFeature);
						features.add(itemFeature);
						featureMap = buildFeatureMap(features);
						if(!checkPart(5, orderLineWKey, 9, featureMap, logic)) {
							stop = true;
							break;
						}
					}
					if (stop)
						break;
				}
				if (!stop)
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
		
		try {
			part = (int) logic.classifyInstance(key);
		} catch (Exception e) {
			System.out.println("Error on classifying instance");
			e.printStackTrace();
		}
		
		// check if logcal or remote
		if (currentPart == -1) {
			currentPart = part;
			return true;
		}
		else if (currentPart == part) {
			return true;
		}
		else {
			if (table == 8 || (table == 1 && w == 1)) {
				return true;
			}
			remote++;
			return false;
		}
	}
	
	// method that returns true if an access is local or false if it is remote
	public boolean checkPart(int txProfile, long key, int table, HashMap<String, Integer> features, LinkedHashMap<Integer,LinkedHashMap<Split, Integer>> logic) {
		// part the access belongs to
		int part = -1;
		// query the map to get the rules for correct txProfile
		LinkedHashMap<Split, Integer> rules = logic.get(txProfile);
		
		// extra situation for delivery in 1w workload
		if (txProfile == 3 && VertexPhi.getScalingFactorW() == 1) {
			part = 1;
		}
		else {
			// given the rules just need to query them in order
			for (Map.Entry<Split, Integer> rule : rules.entrySet()) {
				if (rule.getKey().query(key, table, features))
					part = rule.getValue();
			}
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
			//System.out.println("Table: " + table + " - Remote key: " + key);
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
