package thesis;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;

public class TPCCWorkloadGenerator {
	
	// scaling factors
	private static int w = 100;
	private static int d = 10;
	private static int id = 100000;
	private static int c = 3000;
	
	// current partition for tx
	static private long currentPart = -1;
	
	// no LOCAL transactions (single part)
	static int local = 0;
	// no REMOTE transactions (multi part)
	static int remote = 0;
	
	// testmode
	static int mode = 0;
	// num parts
	static long parts = 10;
	
	// workload generator for Schism
	public static void buildSchismTrace(int noTxs) {
		// number of txs generated
		int generatedTxs = 0;
		
		try {
			// create a file for Weka training
			File schism = new File("schism.txt");
			schism.createNewFile();
			FileWriter schismWriter = new FileWriter("schism.txt");
			// create the number of desired transactions in a trace
			while (generatedTxs < noTxs) {
				// increment genTxs
				generatedTxs++;
				// string to hold line for trace file
				String traceLine = "";
				// roll the dice to know which tx to generate
				int randomNum = ThreadLocalRandom.current().nextInt(0, 100);
				// new order txs
				if (randomNum < 45) {
					// generate random warehouse 1
					long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
					// generate supply warehouse 1% remote
					long supplyW = randW;
					long randSupplyW = ThreadLocalRandom.current().nextInt(0, 100);
					// generate 1% remote new orders
					if (w > 1 && randSupplyW == 50) {
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
						traceLine += " i" + randItem;
						// generate a order line warehouse key 9
						traceLine += " w" + supplyW + "i" + randItem;
						// generate order line key 7
						traceLine += " w" + randW + "d" + randD + "o" + randO + "l" + i;; 
					}
					traceLine += "\n";
					schismWriter.append(traceLine);
				}
				// payment txs
				else if (randomNum < 88) {
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
				else if (randomNum < 92) {
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
				else if (randomNum < 96) {
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
	
	
	
	public static void evaluateCatalyst(int noTxs) {
		// number of txs generated
		int generatedTxs = 0;
		// reset counters
		local = 0;
		remote = 0;
		//generate txs until desired number
		while (generatedTxs < noTxs) {
			// part for current tx
			currentPart = -1;
			// increase no generated txs
			generatedTxs++;
			// boolean variable to stop if remote tx is found
			boolean stop = false;
			// roll the dice to know which tx to generate
			int randomNum = ThreadLocalRandom.current().nextInt(0, 100);
			// new order txs
			if (randomNum < 45) {
				long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
				long randD = ThreadLocalRandom.current().nextInt(0, d);
				long randC = ThreadLocalRandom.current().nextInt(0, c);
				// generate random warehouse 1
				long warehouseKey = randW;
				if(!checkPart(warehouseKey, 1, randW, -1)) continue;
				// generate supply warehouse 1% remote
				long supplyW = warehouseKey;
				long randSupplyW = ThreadLocalRandom.current().nextInt(0, 100);
				// generate 1% remote new orders
				if (w > 1 && randSupplyW == 50) {
					supplyW = ThreadLocalRandom.current().nextInt(0, w);
				}
				// generate random district 2
				long districtKey = randD + (randW * 100);
				if(!checkPart(districtKey, 2, randW, randD)) continue;
				// generate random customer 3
				long customerKey = randD + (randW * 100) + (randC * 10000);
				if(!checkPart(customerKey, 3, randW, randD)) continue;
				// get order number and increment counter 5 6
				long randO = ThreadLocalRandom.current().nextInt(0, c);
				long orderKey = randD + (randW * 100) + (randO * 10000);
				if(!checkPart(orderKey, 5, randW, randD)) continue;
				long newOrderKey = randD + (randW * 100) + (randO * 10000);
				if(!checkPart(newOrderKey, 6, randW, randD)) continue;
				// generate number of items in order, between 5 and 15
				long randNoItems = ThreadLocalRandom.current().nextInt(5, 15);
				// iterate over the the order items
				for (int i = 0; i < randNoItems; i++) {
					// generate a random item 8
					long randItem = ThreadLocalRandom.current().nextInt(0, id);
					long itemKey = randItem;
					if(!checkPart(itemKey, 8, randW, randD)) {
						stop = true;
						break;
					}
					// generate a order line warehouse key 9
					long orderLineWKey = supplyW + (randItem * 100);
					if(!checkPart(orderLineWKey, 9, supplyW, -1)) {
						stop = true;
						break;
					}
					// generate order line key 7
					long orderLineKey = Long.valueOf(randD + (randW * 100) + (randO * 1000000) + (i * 10000));
					if(!checkPart(orderLineKey, 7, randW, randD)) {
						stop = true;
						break;
					}
				}
				if (!stop)
					local++;
			}
			// payment txs
			else if (randomNum < 88) {
				// 85% of payments the customer belongs to local warehouse
				long localCustomer = ThreadLocalRandom.current().nextInt(0, 100);
				if (localCustomer <= 85) {
					// generate random warehouse 1
					long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
					long randD = ThreadLocalRandom.current().nextInt(0, d);
					long randC = ThreadLocalRandom.current().nextInt(0, c);
					long warehouseKey = randW;
					if(!checkPart(warehouseKey, 1, randW, -1)) continue;
					// generate random district 2
					long districtKey = randD + (randW * 100);
					if(!checkPart(districtKey, 2, randW, randD)) continue;
					// generate random customer 3
					long customerKey = randD + (randW * 100) + (randC * 10000);
					if(!checkPart(customerKey, 3, randW, randD)) continue;
					// generate history 4
					long historyKey = randD + (randW * 100) + (randC * 10000);
					if(!checkPart(historyKey, 4, randW, randD)) continue;
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
					if(!checkPart(warehouseKey, 1, randW, -1)) continue;
					// generate random district 2
					long districtKey = randD + (randW * 100);
					if(!checkPart(districtKey, 2, randW, randD)) continue;
					// generate random customer 3, on a potentially remote warehouse
					long customerKey = randD + (randW2 * 100) + (randC * 10000);
					if(!checkPart(customerKey, 3, randW2, randD)) continue;
					// generate history 4, on a potentially remote warehouse
					long historyKey = randD + (randW2 * 100) + (randC * 10000);
					if(!checkPart(historyKey, 4, randW, randD)) continue;
					local++;
				}
			}
			// delivery txs
			else if (randomNum <= 92) {
				// generate random warehouse 1
				long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
				// iterate over all districts
				for (long i = 0; i < d; i++) {
					// generate random customer 3
					long randC = ThreadLocalRandom.current().nextInt(0, c);
					long customerKey = i + (randW * 100) + (randC * 10000);
					if(!checkPart(customerKey, 3, randW, i)) {
						stop = true;
						break;
					}
					// generate random order
					long order = ThreadLocalRandom.current().nextInt(0, c);
					long orderKey = i + (randW * 100) + (order * 10000);
					if(!checkPart(orderKey, 5, randW, i)) {
						stop = true;
						break;
					}
					long newOrderKey = i + (randW * 100) + (order * 10000);
					if(!checkPart(newOrderKey, 6, randW, i)) {
						stop = true;
						break;
					}
					// generate number of items in order, between 5 and 15
					long randNoItems = ThreadLocalRandom.current().nextInt(4, 15);
					// iterate over the the order items
					for (long j = 0; j < randNoItems; j++) {
						// generate order line key 7
						long orderLineKey = Long.valueOf(i + (randW * 100) + (order * 1000000) + (j * 10000));
						if(!checkPart(orderLineKey, 7, randW, i)) {
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
			else if (randomNum <= 96) {
				// generate random warehouse 1
				long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
				// generate random district 2
				long randD = ThreadLocalRandom.current().nextInt(0, d);
				// generate random customer 3
				long randC = ThreadLocalRandom.current().nextInt(0, c);
				long customerKey = randD + (randW * 100) + (randC * 10000);
				if(!checkPart(customerKey, 3, randW, randD)) continue;
				// access order 6
				long orderKey = randD + (randW * 100) + (randC * 10000);
				if(!checkPart(orderKey, 5, randW, randD)) continue;
				// generate number of items in order, between 5 and 15
				long randNoItems = ThreadLocalRandom.current().nextInt(4, 15);
				for (int i = 0; i < randNoItems; i++) {
					// generate order line key 7
					long orderLineKey = Long.valueOf(randD + (randW * 100) + (randC * 1000000) + (i * 10000));
					if(!checkPart(orderLineKey, 7, randW, randD)) {
						break;
					}
				}
				local++;
			}
			// stock level tx
			else {
				// generate random warehouse 1
				long randW = (w == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, w));
				// generate random district 2
				long randD = ThreadLocalRandom.current().nextInt(0, d);
				long districtKey = randD + (randW * 100);
				if(!checkPart(districtKey, 2, randW, randD)) continue;
				for (long i = 0; i < 20; i++) {
					// generate random order
					long randO = ThreadLocalRandom.current().nextInt(0, c);
					long randNoItems = ThreadLocalRandom.current().nextInt(5, 15);
					for (long j = 0; j < randNoItems; j++) {
						// generate order line key 7
						long orderLineKey = Long.valueOf(randD + (randW * 100) + (randO * 1000000) + (j * 10000));
						if(!checkPart(orderLineKey, 7, randW, randD)) {
							stop = true;
							break;
						}
						// generate a random item 8
						long randItem = ThreadLocalRandom.current().nextInt(0, id);
						// generate a order line warehouse key 9
						long orderLineWKey = randW + (randItem * 100);
						if(!checkPart(orderLineWKey, 9, randW, -1)) {
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
	
	// method that returns true if an access is local or false if it is remote
	public static boolean checkPart(long key, int table, long wid, long did) {
		// catalyst testing
		if (mode == 0) {
			// get part for this access
			//int part = this.partitioner.lookupKey(String.valueOf(key), String.valueOf(table));
			// plug part function here
			//int part = (wid <= 4 ? 1 : 2);
			
						
			long part;
			
			part = 0;
			
			
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
		else {
			long part = key % parts;
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
		
	}
	
	public static void main(String[] args) {
		System.out.println("NO warehouses: " + w);
		System.out.println("NO parts: " + parts);
		
		// evaluate Catalyst
		evaluateCatalyst(100);
		evaluateCatalyst(1000);
		evaluateCatalyst(100000);
		// swap to hash part
		mode = 1;
		evaluateCatalyst(100);
		evaluateCatalyst(1000);
		evaluateCatalyst(100000);
	}

}
