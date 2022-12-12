package thesis;


import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Random;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.group.Grouper;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;

public class TPCC {
	
	// node Id for routing etc
	private int nodeId;
	
	// infinispan parameters
	private final EmbeddedCacheManager cacheManager;
	private final TransactionManager transactionManager;
	private Cache<String, ArrayList<String>> cache;
	private Cache<String, ArrayList<String>> replCache;
	private final ClusterListener listener;
	
	// system parameters
	private static int experimentNo;
	private static int systemNo;
	
	// TPCC parameters
	private int NUM_WAREHOUSES = -1;
	private final int NUM_DISTRICTS = 10;
	private final int NUM_CUSTOMERS = 3000;
	private final int NUM_ORDERS = 3000;
    private final long MAX_ITEM = 100000;
    
	// local vars for table population
	private boolean newOrder = false;
	private int customerIds[];

	// random parameters
    private final int NULL_NUMBER = -1;
    private long POP_C_LAST = NULL_NUMBER;
    private long POP_C_ID = NULL_NUMBER;
    private long POP_OL_I_ID = NULL_NUMBER;
    private final int MIN_C_LAST = 0;
    private final int MAX_C_LAST = 999;
    private int A_C_LAST = 255;
    private int A_OL_I_ID = 8191;
    private int A_C_ID = 1023;
    private final int LIMIT_ORDER = 2101;
    public final static String[] nameTokens = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
    private final String[] C_LAST = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
	private final int DEFAULT_RADIX = 10;
	private final int DEFAULT_MINL = 65;
    private final int DEFAULT_MAXL = 90;
    private final int DEFAULT_MINN = 48;
    private final int DEFAULT_MAXN = 57;
    private final int S_DATA_MINN = 26;
    private final int S_DATA_MAXN = 50;
    private final String ORIGINAL = "ORIGINAL";
    private final String ZIP = "11111";
	private final int unicode[][] = { {65, 126}, {192, 259}};
    private Random randUniform = new Random(System.nanoTime());
    private Random randNonUniform = new Random(System.nanoTime());
	private Random rand = new Random(System.nanoTime());
	
	// seeds for random values
	private long seedC_id = 501;
	private long seedC_last = 132;
	private long seedI_id = 5121;

	
	// build a cache for TPCC execution
	public TPCC(int systemNo, int experimentNo, int nodeId) {
		
		// set node Id
		this.nodeId = nodeId;
		
		// set experiment number and systemNo
		TPCC.experimentNo = experimentNo;
		TPCC.systemNo = systemNo;
		
		// setup number of warehouses
		if (experimentNo == 1) {
			this.NUM_WAREHOUSES = 1;
		}
		else if (experimentNo == 2) {
			this.NUM_WAREHOUSES = 2;
		}
		else {
			this.NUM_WAREHOUSES = -1;
		}
		
		GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
		global.cacheContainer().statistics(true);
		// TODO Might need serialization here
		
		// created a cache manager, can now have multiple caches
		this.cacheManager = new DefaultCacheManager(global.build());
		listener = new ClusterListener(2);
	    cacheManager.addListener(listener);
	    
	    // builder for non replicated cache
		ConfigurationBuilder config = new ConfigurationBuilder();
		config.clustering().cacheMode(CacheMode.DIST_SYNC)
			.hash().numOwners(1).groups().enabled().addGrouper(new TPCC.KeyGrouper());
		config.locking()
	    	.isolationLevel(IsolationLevel.READ_COMMITTED);
		config.transaction()
		    .lockingMode(LockingMode.OPTIMISTIC)
		    .autoCommit(true) // TODO dobule check this
		    .completedTxTimeout(60000)
		    .transactionMode(TransactionMode.NON_TRANSACTIONAL)
		    .useSynchronization(false)
		    .notifications(true)
		    .reaperWakeUpInterval(30000)
		    .cacheStopTimeout(30000)
		    .transactionManagerLookup(new GenericTransactionManagerLookup())
		    .recovery()
		    .enabled(false)
		    .recoveryInfoCacheName("__recoveryInfoCacheName__");
		config.statistics().enable();
		this.cacheManager.defineConfiguration("tpcc", config.build());
		this.cache = cacheManager.getCache("tpcc");
		this.transactionManager = cache.getAdvancedCache().getTransactionManager();
		
		// builder for replicated cache
		ConfigurationBuilder configReplicated = new ConfigurationBuilder();
		configReplicated.clustering().cacheMode(CacheMode.REPL_SYNC);
		/*configReplicated.locking()
    		.isolationLevel(IsolationLevel.READ_COMMITTED);
		configReplicated.transaction()
		    .lockingMode(LockingMode.OPTIMISTIC)
		    .autoCommit(true)
		    .completedTxTimeout(60000)
		    .transactionMode(TransactionMode.NON_TRANSACTIONAL)
		    .useSynchronization(false)
		    .notifications(true)
		    .reaperWakeUpInterval(30000)
		    .cacheStopTimeout(30000)
		    .transactionManagerLookup(new GenericTransactionManagerLookup())
		    .recovery()
		    .enabled(false)
		    .recoveryInfoCacheName("__recoveryInfoCacheName__"); */
		this.cacheManager.defineConfiguration("repltpcc", configReplicated.build());
		this.replCache = cacheManager.getCache("repltpcc");
		
		
		
		if (nodeId == 0) initCache();
		
		
		// place in the cache the information that this node is ready
		String nodeReadyKey = "10," + nodeId;
		ArrayList<String> nodeReadyValue = new ArrayList<>();
		nodeReadyValue.add(String.valueOf(nodeId));
		cache.put(nodeReadyKey, nodeReadyValue);
		
		System.out.println("Node ready: " + cache.get(nodeReadyKey).get(0));
		
		// check if cluster is ready to begin transaction execution
		while (true) {
			// first experiment has 2 nodes
			if (experimentNo == 1) {
				boolean allReady = true;
				for (int i = 0; i < 2; i++) {
					nodeReadyKey = "10," + i;
					nodeReadyValue = cache.get(nodeReadyKey);
					if (nodeReadyValue == null) {
						allReady = false;
						break;	
					}
				}
				// if every node is ready then proceed, else continue loop
				if (allReady) break;
			}
			else if (experimentNo == 2) {
				boolean allReady = true;
				for (int i = 0; i < 2; i++) {
					nodeReadyKey = "10," + i;
					nodeReadyValue = cache.get(nodeReadyKey);
					if (nodeReadyValue == null) {
						allReady = false;
						break;	
					}
				}
				// if every node is ready then proceed, else continue loop
				if (allReady) break;
			}
			else {
				break;
			}
		}
		
		System.out.println("Ready for transaction execution!");
		
		Instant start = Instant.now();
		int cycleTxs = 0;
		
		Instant experimentBegin = Instant.now();
		
		Instant current = Instant.now();

		boolean test = true;
				
		System.out.println("Hits: " + this.cache.getAdvancedCache().getStats().getHits());
		
		System.out.println("Misses: " + this.cache.getAdvancedCache().getStats().getMisses());
		
		while (true && Duration.between(experimentBegin, current).toMinutes() <= 1) {
			try {
				executeTransaction();
				Thread.sleep(10);
			} catch (SecurityException | IllegalStateException | NotSupportedException | SystemException
					| RollbackException | HeuristicMixedException | HeuristicRollbackException e) {
				System.out.println("Error during transaction execution!");
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			cycleTxs++;
			current = Instant.now();
			if (Duration.between(start, current).toMillis() >= 3000) {
				//System.out.println("Evaluating thorughput");
				double txPS = cycleTxs / (Duration.between(start, current).toMillis() / 1000);
				//System.out.println("Executed " + txPS + "/s");
				System.out.println(txPS);
				cycleTxs = 0;
				start = Instant.now();
			}
			if (test && Duration.between(experimentBegin, current).toMillis() >= 15000) {
				System.out.println("Hits: " + this.cache.getAdvancedCache().getStats().getHits());
				System.out.println("Misses: " + this.cache.getAdvancedCache().getStats().getMisses());
				test = false;
			}
				
		}
		
		System.out.println("Hits: " + this.cache.getAdvancedCache().getStats().getHits());
				
		System.out.println("Misses: " + this.cache.getAdvancedCache().getStats().getMisses());
		
		System.out.println("Write: " + this.cache.getAdvancedCache().getStats().getAverageWriteTimeNanos());
		
		System.out.println("Read: " + this.cache.getAdvancedCache().getStats().getAverageReadTimeNanos());
		


		
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		cacheManager.stop();
		
		System.exit(0);
	}
	
	// generate transaction
	private void executeTransaction() throws NotSupportedException, SystemException, SecurityException, IllegalStateException, RollbackException, HeuristicMixedException, HeuristicRollbackException {
		// get a warehouse id for this tx
		long terminalWId = getRoutingWarehouse(this.nodeId, TPCC.experimentNo);
		// pick transaction
		int txType = generateRandNum(1,100);
		// new order
		if (txType <= 45) {
			long districtId = getRoutingDistrict(this.nodeId, TPCC.experimentNo);
			long customerId = nonUniformRandom(seedC_id, A_C_ID, 1, NUM_CUSTOMERS);
			// generate item arrays
			int numItems = (int) randomNumber(5,15);
			long[] itemIDs = new long[numItems];
			long[] supplierWarehouseIDs = new long[numItems];
            long[] orderQuantities = new long[numItems];
            int allLocal = 1;
            // fill item array
            for(int i = 0; i < numItems; i++) {
            	itemIDs[i] = nonUniformRandom(seedI_id, A_OL_I_ID, 1, MAX_ITEM);
            	if (randomNumber(1, 100) > 1 || NUM_WAREHOUSES == 1) {
            		supplierWarehouseIDs[i] = terminalWId;
            	}
            	else {
            		do {
            			supplierWarehouseIDs[i] = randomNumber(1, NUM_WAREHOUSES);
            		}
            		while (supplierWarehouseIDs[i] == terminalWId);
            		allLocal = 0;
            	}
            	orderQuantities[i] = randomNumber(1,10);
            }
            transactionManager.begin();
            newOrderTransaction(terminalWId, districtId, customerId, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities);
            transactionManager.commit();
		}
		// payment
		else if (txType <= 88) {
			long districtId = getRoutingDistrict(this.nodeId, TPCC.experimentNo);
			long local = randomNumber(1, 100);
			long customerDistrictId;
			long customerWarehouseId;
			// local scenario
			if (local <= 85) {
				customerDistrictId = districtId;
				customerWarehouseId = terminalWId;
			}
			else {
				customerDistrictId = getRoutingDistrict(this.nodeId, TPCC.experimentNo);
				do {
					customerWarehouseId = randomNumber(1, NUM_WAREHOUSES);
				}
				while (customerWarehouseId == terminalWId && NUM_WAREHOUSES > 1);
			}
			long customerType = randomNumber(1,100);
			boolean customerByName;
			String customerLastName = null;
			long customerId = -1;
			// customerByName scenario
			if (customerType <= 0) {
				customerByName = true;
				customerLastName = lastName((int) nonUniformRandom(seedC_last, A_C_LAST, 0, MAX_C_LAST));
			}
			else {
				customerByName = false;
				customerId = nonUniformRandom(seedC_id, A_C_ID, 1, NUM_CUSTOMERS);
			}
			double amount = randomNumber(100, 500000)/100.0;
			transactionManager.begin();
			paymentTransaction(terminalWId, customerWarehouseId, amount, districtId, customerDistrictId, customerId, customerLastName, customerByName);
			transactionManager.commit();
		}
		// order status
		else if (txType <= 92) {
			long districtId = getRoutingDistrict(this.nodeId, TPCC.experimentNo);
			long customerType = randomNumber(1,100);
			boolean customerByName;
			String customerLastName = null;
			long customerId = -1;
			// customerByName scenario
			if (customerType <= 0) {
				customerByName = true;
				customerLastName = lastName((int) nonUniformRandom(seedC_last, A_C_LAST, 0, MAX_C_LAST));
				//customerLastName = c_last();
			}
			else {
				customerByName = false;
				customerId = nonUniformRandom(seedC_id, A_C_ID, 1, NUM_CUSTOMERS);
			}
			transactionManager.begin();
			orderStatusTransaction(terminalWId, districtId, customerId, customerLastName, customerByName);
			transactionManager.commit();
		}
		// stock level
		else if (txType <= 96) {
			long districtId = getRoutingDistrict(this.nodeId, TPCC.experimentNo);
			int stockThreshold = generateRandNum(10,20);
			transactionManager.begin();
			stockLevelTransaction(terminalWId, districtId, stockThreshold);
			transactionManager.commit();
		}
		// delivery
		else {
			if ((experimentNo == 1 && nodeId == 0) || experimentNo != 1) {
				int o_carrier_id = generateRandNum(1, 10);
				Date ol_delivery_d = new Date(System.currentTimeMillis());
				transactionManager.begin();
				deliveryTransaction(terminalWId, o_carrier_id, ol_delivery_d);
				transactionManager.commit();
			}
		}
	}
	
	// conditional routing warehouse gen
	private long getRoutingWarehouse(int nodeId, int experimentNo) {
		// 1w2p
		if (experimentNo == 1) {
			return 1;
		}
		// 2w2p
		if (experimentNo == 2) {
			if (nodeId == 0) {
				return 1;
			}
			if (nodeId == 1) {
				return 2;
			}
		}
		return randomNumber(1,NUM_WAREHOUSES);
	}
	
	// conditional routing district gen
	private long getRoutingDistrict(int nodeId, int experimentNo) {
		// 1w2p
		if (experimentNo == 1) {
			if (nodeId == 0)
				return randomNumber(1,5);
			else
				return randomNumber(6,10);
		}
		return randomNumber(1,NUM_DISTRICTS);
	}
	
	// payment transaction
	private void paymentTransaction(long w_id, long c_w_id, double h_amount, long d_id, long c_d_id, long c_id, String c_last, boolean c_by_name) {
		// select the warehouse
		String warehouseKey = "1,w" + w_id;
		ArrayList<String> warehouse = cache.get(warehouseKey);
		// if the key does not exist return an error
		if (warehouse == null) System.out.println("Payment: warehouse key not found!");
		// increase the ytd amount
		double amountYtd = Double.valueOf(warehouse.get(8)) + h_amount;
		warehouse.set(8, String.valueOf(amountYtd));
		// select the district
		String districtKey = "2,w" + w_id + "d" + d_id;
		ArrayList<String> district = cache.get(districtKey);
		// if the key does not exist return an error
		if (district == null) System.out.println("Payment: district key not found!");
		// increase the ytd amount
		double amountDYtd = Double.valueOf(district.get(9)) + h_amount;
		district.set(9, String.valueOf(amountDYtd));
		

		// case 1, customer selected based on customer name
		if (c_by_name) {
			// list of customers
			ArrayList<ArrayList<String>> customerList = new ArrayList<>();
			// select the customers with matching last name
			for (int i = 1; i <= NUM_CUSTOMERS; i++) {
				// select from all the customers
				String customerKey = "3,w" + c_w_id + "d" + c_d_id + "c" + i;
				ArrayList<String> customer = cache.get(customerKey);
				// filter for matching c_last
				if (customer.get(5).equals(c_last)) {
					customerList.add(customer);
				}
			}
			
			if (!customerList.isEmpty()) {
				// sort customer list in ascending order
				customerList = sortArray(customerList);
				// select n/2 rounded up to next integer
				int numSelected = (customerList.size() % 2 == 1 ? customerList.size()/2 + 1 : customerList.size()/2);
				// perform operations
				for (int i = 0; i < numSelected; i++) {
					// retrieve the customer
					ArrayList<String> selCustomer = customerList.get(i);
					// decrease c_balance by h_amount
					double newBalance = Double.valueOf(selCustomer.get(16)) - h_amount;
					selCustomer.set(16, String.valueOf(newBalance));
					// increase c_ytd_payment by h_amount
					double newYtdPayment = Double.valueOf(selCustomer.get(17)) + h_amount;
					selCustomer.set(17, String.valueOf(newYtdPayment));
					// increase c_payment_cnt by 1
					double newCPaymentCnt = Double.valueOf(selCustomer.get(18)) + 1;
					selCustomer.set(18, String.valueOf(newCPaymentCnt));
					// credit check
					if(selCustomer.get(13).equals("BC")) {
						// get current c_data
						String c_data = selCustomer.get(20);
						String c_new_data = selCustomer.get(2) + " " + c_d_id + " " + c_w_id + " " + d_id + " " + w_id + " " + h_amount + " |";
						if(c_data.length() > c_new_data.length()) {
							c_new_data += c_data.substring(0, c_data.length() - c_new_data.length());
						}
						else {
							c_new_data += c_data;
						}
						if(c_new_data.length() > 500) c_new_data = c_new_data.substring(0, 500);
						selCustomer.set(20, c_new_data);
					}
					// update history
					String w_name = cache.get("1,w" + w_id).get(1);
					String d_name = cache.get("2,w" + w_id + "d" + d_id).get(2);
					String h_data = w_name + "    " + d_name;
					String historyKey = "4,w" + c_w_id + "d" + c_d_id + "h" + selCustomer.get(2);
					ArrayList<String> historyValue = cache.get(historyKey);
					if (historyValue == null) System.out.println("Payment: history key not found!");
					historyValue.set(5, h_data);
					cache.put(historyKey, historyValue);
					// write the results to the db
					String key = "3,w" + c_w_id + "d" + c_d_id + "c" + selCustomer.get(2);
					cache.put(key, selCustomer);
				}
			}
			else {
				System.out.println("Payment: customer(name) key not found!");
			}
		}
		else {
			// retrieve the customer
			String key = "3,w" + c_w_id + "d" + c_d_id + "c" + c_id;
			ArrayList<String> selCustomer = cache.get(key);
			if (selCustomer == null) System.out.println("Payment: customer(id) key not found!");
			// decrease c_balance by h_amount
			double newBalance = Double.valueOf(selCustomer.get(16)) - h_amount;
			selCustomer.set(16, String.valueOf(newBalance));
			// increase c_ytd_payment by h_amount
			double newYtdPayment = Double.valueOf(selCustomer.get(17)) + h_amount;
			selCustomer.set(17, String.valueOf(newYtdPayment));
			// increase c_payment_cnt by 1
			double newCPaymentCnt = Double.valueOf(selCustomer.get(18)) + 1;
			selCustomer.set(18, String.valueOf(newCPaymentCnt));
			// credit check
			if(selCustomer.get(13).equals("BC")) {
				// get current c_data
				String c_data = selCustomer.get(20);
				String c_new_data = selCustomer.get(2) + " " + c_d_id + " " + c_w_id + " " + d_id + " " + w_id + " " + h_amount + " |";
				if(c_data.length() > c_new_data.length()) {
					c_new_data += c_data.substring(0, c_data.length() - c_new_data.length());
				}
				else {
					c_new_data += c_data;
				}
				if(c_new_data.length() > 500) c_new_data = c_new_data.substring(0, 500);
				selCustomer.set(20, c_new_data);
			}
			// update history
			String w_name = cache.get("1,w" + w_id).get(1);
			String d_name = cache.get("2,w" + w_id + "d" + d_id).get(2);
			String h_data = w_name + "    " + d_name;
			String historyKey = "4,w" + c_w_id + "d" + c_d_id + "h" + selCustomer.get(2);
			ArrayList<String> historyValue = cache.get(historyKey);
			if (historyValue == null) System.out.println("Payment: history key not found!");
			historyValue.set(5, h_data);
			cache.put(historyKey, historyValue);
			// write the results to the db
			cache.put(key, selCustomer);
		}
		
		
	}
	
	// new order transaction
	private void newOrderTransaction(long w_id, long d_id, long c_id, int o_ol_cnt, int o_all_local, long[] itemIDs, long[] supplierWarehouseIDs, long[] orderQuantities) {
		// retrieve the warehouse key
		String warehouseKey = "1,w" + w_id;
		cache.get(warehouseKey);
		// retrieve the district key
		String districtKey = "2,w" + w_id + "d" + d_id;
		ArrayList<String> districtValue = cache.get(districtKey);
		// if the key does not exist return an error
		if (districtValue == null) System.out.println("NewOrder: district key not found!");
		// increment next order id for district
		int nextOrder = Integer.valueOf(districtValue.get(10));
		districtValue.set(10, String.valueOf(nextOrder + 1));
		cache.put(districtKey, districtValue);
		// retrieve the customer key
		String customerKey = "3,w" + w_id + "d" + d_id + "c" + c_id;
		ArrayList<String> customerValue = cache.get(customerKey);
		// if the key does not exist return an error
		if (customerValue == null) System.out.println("NewOrder: customer key not found!");
		// INDEX 1 - insert latest order id for a customer
		if (customerValue.get(21) == null || Integer.valueOf(customerValue.get(21)) <= nextOrder) {
			customerValue.set(21, String.valueOf(nextOrder));
			cache.put(customerKey, customerValue);
		}
		// insert in new order 
		String newOrderKey = "5,w" + w_id + "d" + d_id + "no" + nextOrder;
		ArrayList<String> newOrderValue = null;
		// if the key does not exist, create it else return an error
		if (newOrderValue == null) {
			newOrderValue = new ArrayList<String>();
			newOrderValue.add(0,String.valueOf(nextOrder));
			newOrderValue.add(1,String.valueOf(d_id));
			newOrderValue.add(2,String.valueOf(w_id));
		}
		else {
			System.out.println("New Order: new order key already exists! : " + newOrderKey);
		}
		cache.put(newOrderKey, newOrderValue);
		// INDEX 2 - insert smallest order id for a given district if it doesn't exist yet
		String smallestOrderIdIndex = "10,w" + w_id + "d" + d_id;
		// if the index does not exist, create it
		if (cache.get(smallestOrderIdIndex) == null) {
			ArrayList<String> smallestOrderIdIndexValue = new ArrayList<>();
			smallestOrderIdIndexValue.add(String.valueOf(nextOrder));
			cache.put(smallestOrderIdIndex, smallestOrderIdIndexValue);
		}
		// insert in order
		String orderKey = "6,w" + w_id + "d" + d_id + "o" + nextOrder;
		ArrayList<String> orderValue = null;
		// if the key does not exist create it, it it does return an error
		if (orderValue == null) {
			Date date = new Date((new Date()).getTime());
			orderValue = new ArrayList<>();
			orderValue.add(0, String.valueOf(nextOrder));
			orderValue.add(1, String.valueOf(d_id));
			orderValue.add(2, String.valueOf(w_id));
			orderValue.add(3, String.valueOf(c_id));
			orderValue.add(4, String.valueOf(date.toString()));
			orderValue.add(5, String.valueOf(-1));
			orderValue.add(6, String.valueOf(o_ol_cnt));
			orderValue.add(7, String.valueOf(o_all_local));
		}
		else {
			System.out.println("New Order: order key already exists! : " + orderKey);
		}
		cache.put(orderKey, orderValue);
		// for each item in the order
		for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
			// array reads for auxiliary values
			long ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
			long ol_i_id = itemIDs[ol_number - 1];
			long ol_quantity = orderQuantities[ol_number - 1];
			
			// item table read
			String itemKey = "8,i" + ol_i_id;
			ArrayList<String> itemValue = replCache.get(itemKey);
			// if the key does not exist return an error
			if (itemValue == null) System.out.println("NewOrder: item key not found!" + itemKey);
	
			// stock table 
			String stockKey = "9,w" + ol_supply_w_id + "i" + ol_i_id;
			ArrayList<String> stockValue = cache.get(stockKey);
			// if the key does not exist return an error
			if (stockValue == null) System.out.println("NewOrder: stock key not found!");
			if (Long.valueOf(cache.get(stockKey).get(2)) - ol_quantity >= 10) {
				long newQuantity = Long.valueOf(cache.get(stockKey).get(2)) - ol_quantity;
				stockValue.set(2, String.valueOf(newQuantity));
			}
			else {
				long newQuantity = (Long.valueOf(cache.get(stockKey).get(2)) - ol_quantity) + 91;
				stockValue.set(2, String.valueOf(newQuantity));
			}
			
			int s_remote_cnt_increment;
			// check if order line is to a remote warehouse
			if (ol_supply_w_id == w_id) {
				s_remote_cnt_increment = 0;
			}
			else {
				s_remote_cnt_increment = 1;
			}
			long newYtd = Long.valueOf(stockValue.get(13)) + ol_quantity;
			stockValue.set(13, String.valueOf(newYtd));
			int s_order_cntNew = Integer.valueOf(stockValue.get(14)) + 1;
			stockValue.set(14, String.valueOf(s_order_cntNew));
			int s_remote_cntNew = Integer.valueOf(stockValue.get(15)) + s_remote_cnt_increment;
			stockValue.set(15, String.valueOf(s_remote_cntNew));
			cache.put(stockKey, stockValue);
			
			float ol_amount = ol_quantity * Float.valueOf(itemValue.get(3));
			
			// orderline insert
			String orderLineKey = "7,w" + w_id + "d" + d_id + "o" + nextOrder + "l" + ol_number;
			String ol_dist_info = stockValue.get((int) d_id + 2);
			ArrayList<String> orderLineValue = null;
			// if the key does exit, return an error, else create a new order line key
			if (orderLineValue == null) {
				//System.out.println("NewOrder: order line key not found, creating it!");
				orderLineValue = new ArrayList<>();
				orderLineValue.add(0, String.valueOf(nextOrder));
				orderLineValue.add(1, String.valueOf(d_id));
				orderLineValue.add(2, String.valueOf(w_id));
				orderLineValue.add(3, String.valueOf(ol_number));
				orderLineValue.add(4, String.valueOf(ol_i_id));
				orderLineValue.add(5, String.valueOf(ol_supply_w_id));
				orderLineValue.add(6, null);
				orderLineValue.add(7, String.valueOf(ol_quantity));
				orderLineValue.add(8, String.valueOf(ol_amount));
				orderLineValue.add(9, String.valueOf(ol_dist_info));
			}
			else {
				System.out.println("New order: order line key already exists!");
			}
			cache.put(orderLineKey, orderLineValue);
		}
	}
		
	// delivery transaction
	private void deliveryTransaction(long w_id, int o_carrier_id, Date ol_delivery_d) {
		// go over every district
		for (int i = 1; i <= NUM_DISTRICTS; i++) {
			// get the smallest order id for this district
			String indexQuery = "10,w" + w_id + "d" + i;
			// if the index has not been set for this district then there is no order to deliver
			ArrayList<String> indexValue = cache.get(indexQuery);
			if (indexValue == null) continue;
			// get the smallest order id for the district otherwise
			String o_id = indexValue.get(0);
			// increment the value stored 
			int next_o_id = Integer.valueOf(o_id) + 1;
			indexValue.set(0, String.valueOf(next_o_id));
			cache.put(indexQuery, indexValue);
			// delete the row in new order
			String newOrderKey = "5,w" + w_id + "d" + i + "no" + o_id;
			cache.remove(newOrderKey);
			// update order table
			String orderKey = "6,w" + w_id + "d" + i + "o" + o_id;
			ArrayList<String> orderValue = cache.get(orderKey);
			// if the key does not exist continue, no order to deliver for district
			if (orderValue == null) {
				continue;
			}
			// get the customer id from the order
			String o_c_id = orderValue.get(3);
			// update the carrier id
			orderValue.set(5, String.valueOf(o_carrier_id));
			// retrieve how many items are in the order
			int o_ol_cnt = Integer.valueOf(orderValue.get(6));
			// go over every item in the order and sum the amount
			double sum_ol_amount = 0;
			for(int j = 1; j <= o_ol_cnt; j++) {
				String orderLineKey = "7,w" + w_id + "d" + i + "o" + o_id + "l" + j;
				ArrayList<String> orderLineValue = cache.get(orderLineKey);
				// if the key does not exist return an error
				if (orderLineValue == null) System.out.println("Delivery: order line key not found!: " + orderLineKey + " | " + orderKey + " | " + o_ol_cnt + " | " + j);
				// get current system date for delivery value
				Date current = ol_delivery_d;
				// retrieve the value/quantity and increment the order amount
				sum_ol_amount += (Double.valueOf(orderLineValue.get(8)) * Integer.valueOf(orderLineValue.get(7)));
				// place the order date in the db
				orderLineValue.set(6, current.toString());
				cache.put(orderLineKey, orderLineValue);
			}
			// build the customer key
			String customerKey = "3,w" + w_id + "d" + i + "c" + o_c_id;
			ArrayList<String> customerValue = cache.get(customerKey);
			// if the key does not exist return an error
			if (customerValue == null) System.out.println("Delivery: customer key not found!: " + customerKey);
			// update the customer values
			double newBalance = Double.valueOf(customerValue.get(16)) + sum_ol_amount;
			customerValue.set(16, String.valueOf(newBalance));
			int newDeliveryCnt = Integer.valueOf(customerValue.get(19)) + 1;
			customerValue.set(19, String.valueOf(newDeliveryCnt));
			// store the new customer value
			cache.put(customerKey, customerValue);
		}
	}
	
	// order status transaction
	private void orderStatusTransaction(long w_id, long d_id, long c_id, String c_last, boolean c_by_name) {
		// latest order id for a customer
		int latest_order;
		// case 1, customer selected based on customer name
		if (c_by_name) {
			// list of customers
			ArrayList<ArrayList<String>> customerList = new ArrayList<>();
			// select the customers with matching last name
			for (int i = 1; i <= NUM_CUSTOMERS; i++) {
				// select from all the customers
				String customerKey = "3,w" + w_id + "d" + d_id + "c" + i;
				ArrayList<String> customer = cache.get(customerKey);
				// filter for matching c_last
				if (customer.get(5).equals(c_last)) {
					customerList.add(customer);
				}
			}
			if (!customerList.isEmpty()) {
				// sort customer list in ascending order
				customerList = sortArray(customerList);
				// select n/2 rounded up to next integer
				int numSelected = (customerList.size() % 2 == 1 ? customerList.size()/2 + 1 : customerList.size()/2);
				// perform operations
				for (int i = 0; i < numSelected; i++) {
					// retrieve the customer
					ArrayList<String> selCustomer = customerList.get(i);
					// read the latest order id for the customer from the,  if latest_order is null means the costumer hasn't ordered anything yet
					if (selCustomer.get(21) != null)
						latest_order = Integer.valueOf(selCustomer.get(21));
					else
						continue;
					// build the order key
					String orderKey = "6,w" + w_id + "d" + d_id + "o" + latest_order;
					// read the value
					ArrayList<String> orderValue = cache.get(orderKey);
					if (orderValue == null) System.out.println("OrderStatus: order key not found");
					// get number of items in order
					int ol_cnt = Integer.valueOf(orderValue.get(6));
					// go over all items in the order
					for (int j = 1; j <= ol_cnt; j++) {
						String orderLineKey = "7,w" + w_id + "d" + d_id + "o" + latest_order + "l" + j;
						if (cache.get(orderLineKey) == null) System.out.println("OrderStatus: order line key not found!");
					}
				}
			}
			else {
				System.out.println("OrderStatus: customer(name) key not found!");
			}
		}
		else {
			// retrieve the customer
			String key = "3,w" + w_id + "d" + d_id + "c" + c_id;
			ArrayList<String> selCustomer = cache.get(key);
			if (selCustomer == null) System.out.println("OrderStatus: customer(id) key not found!");
			// read the latest order id for the customer from the,  if latest_order is null means the costumer hasn't ordered anything yet
			if (selCustomer.get(21) != null)
				latest_order = Integer.valueOf(selCustomer.get(21));
			else
				return;
			// build the order key
			String orderKey = "6,w" + w_id + "d" + d_id + "o" + latest_order;
			// read the value
			ArrayList<String> orderValue = cache.get(orderKey);
			if (orderValue == null) System.out.println("OrderStatus: order key not found");
			// get number of items in order
			int ol_cnt = Integer.valueOf(orderValue.get(6));
			// go over all items in the order
			for (int j = 1; j <= ol_cnt; j++) {
				String orderLineKey = "7,w" + w_id + "d" + d_id + "o" + latest_order + "l" + j;
				if (cache.get(orderLineKey) == null) System.out.println("OrderStatus: order line key not found!");
			}
		}
	}
	
	// stock level transaction
	private void stockLevelTransaction(long w_id, long d_id, int threshold) {
		// low stuck products < threshold
		int lowStock = 0;
		// build district key
		String districtKey = "2,w" + w_id + "d" + d_id;
		// read district value
		ArrayList<String> districtValue = cache.get(districtKey);
		// if the key does not exist return an error
		if (districtValue == null) System.out.println("StockLevel: district key not found!");
		// retrieve next order id
		int next_o_id = Integer.valueOf(districtValue.get(10));
		// go over the 20 recent orders
		for (int i = next_o_id - 20; i < next_o_id; i++) {
			// ignore incorrect order ids
			if (i <= 0) continue;
			// build order key
			String orderKey = "6,w" + w_id + "d" + d_id + "o" + i;
			// read order key Value
			ArrayList<String> orderValue = cache.get(orderKey);
			if (orderValue == null) System.out.println("StockLevel: order key not found!");
			// get the order line count value
			int ol_cnt = Integer.valueOf(orderValue.get(6));
			// go over the order lines
			for (int j = 1; j <= ol_cnt; j++) {
				// build order line key
				String orderLineKey = "7,w" + w_id + "d" + d_id + "o" + i + "l" + j;
				// read order line value
				ArrayList<String> orderLineValue = cache.get(orderLineKey);
				if (orderLineValue == null) System.out.println("StockLevel: order line key not found! :" + orderLineKey);
				// get the item of the order
				String ol_i_id = orderLineValue.get(4);
				// build the stock Key
				String stockKey = "9,w" + w_id + "i" + ol_i_id;
				// read the stock value
				ArrayList<String> stockValue = cache.get(stockKey);
				if (stockValue == null) System.out.println("StockLevel: stock key not found!");
				// check the threshold
				if (Integer.valueOf(stockValue.get(2)) < threshold)
					lowStock++;
			}
		}
		
	}
	
	// getter for the TPCC cache
	public ArrayList<String> get(String key) {
		ArrayList<String> value = cache.get(key); 
		if (value != null) {
			return value;
		}
		System.out.println("Key does not exist!");
		return null;
	}
	
	// put for the TPCC cache
	public void put(String key, ArrayList<String> value) {
		cache.put(key, value);
	}
	
	// stop the cache to free resources
	public void freeCache() {
		this.cacheManager.stop();
	}
	
	// cache initialize method
	public void initCache() {
		// need to place every table of TPCC in the cache
		// key structure needs to be: table,key
		
		// init item table
		initItems();
		
		// init warehouse table
		initWarehouse();
	}
	
	// init item table
	public void initItems() {
		System.out.println("Populating item table");
		
		for(long i = 1; i <= MAX_ITEM; i++) {
			String i_id = String.valueOf(i);
			String i_im_id = String.valueOf(generateRandLong(1,10000));
			String i_name = generateAstring(14,24);
			String i_price = String.valueOf(generateRandFloat(1, 100, 2));
			String i_data = s_data();
			
			ArrayList<String> itemValue = new ArrayList<>();
			itemValue.add(0,i_id);
			itemValue.add(1,i_im_id);
			itemValue.add(2,i_name);
			itemValue.add(3,i_price);
			itemValue.add(4,i_data);
			
			String key = "8,i" + i;
			replCache.put(key, itemValue);
		}
	}
	
	// init warehouse table
	public void initWarehouse() {
		System.out.println("Populating warehouse table");
		
		for(int i = 1; i <= NUM_WAREHOUSES; i++) {
			String w_id = String.valueOf(i);
			String w_name = generateAstring(6, 10);
			String w_street_1 = generateAstring(10, 20);
			String w_street_2 = generateAstring(10, 20);
			String w_city = generateAstring(10, 20);
			String w_state = generateLstring(2, 2);
			String w_zip = generateNstring(4, 4) + ZIP;
			String w_tax = String.valueOf(generateRandFloat(Float.valueOf("0.0000").floatValue(), Float.valueOf("0.2000").floatValue(), 4));
			String w_ytd = String.valueOf(Double.valueOf(300000.00).doubleValue());
			
			ArrayList<String> warehouseValue = new ArrayList<>();
			warehouseValue.add(0,w_id);
			warehouseValue.add(1,w_name);
			warehouseValue.add(2,w_street_1);
			warehouseValue.add(3,w_street_2);
			warehouseValue.add(4,w_city);
			warehouseValue.add(5,w_state);
			warehouseValue.add(6,w_zip);
			warehouseValue.add(7,w_tax);
			warehouseValue.add(8,w_ytd);
			
			String key = "1,w" + i;
			cache.put(key, warehouseValue);
			
			initStock(i);
			initDistrict(i);
		}
	}
	
	// init stock table for a given warehouse
	public void initStock(int warehouse) {
		System.out.println("Populating stock table");
		
		for(int i = 1; i <= MAX_ITEM; i++) {
			String s_i_id = String.valueOf(i);
			String s_w_id = String.valueOf(warehouse);
			String s_quantity = String.valueOf(generateRandNum(10,100));
			String s_dist_01 = generateAstring(24,24);
			String s_dist_02 = generateAstring(24,24);
			String s_dist_03 = generateAstring(24,24);
			String s_dist_04 = generateAstring(24,24);
			String s_dist_05 = generateAstring(24,24);
			String s_dist_06 = generateAstring(24,24);
			String s_dist_07 = generateAstring(24,24);
			String s_dist_08 = generateAstring(24,24);
			String s_dist_09 = generateAstring(24,24);
			String s_dist_10 = generateAstring(24,24);
			String s_ytd = String.valueOf(0);
			String s_order_cnt = String.valueOf(0);
			String s_remote_cnt = String.valueOf(0);
			String s_data = s_data();
			
			ArrayList<String> stockValue = new ArrayList<>();
			stockValue.add(0,s_i_id);
			stockValue.add(1,s_w_id);
			stockValue.add(2,s_quantity);
			stockValue.add(3,s_dist_01);
			stockValue.add(4,s_dist_02);
			stockValue.add(5,s_dist_03);
			stockValue.add(6,s_dist_04);
			stockValue.add(7,s_dist_05);
			stockValue.add(8,s_dist_06);
			stockValue.add(9,s_dist_07);
			stockValue.add(10,s_dist_08);
			stockValue.add(11,s_dist_09);
			stockValue.add(12,s_dist_10);
			stockValue.add(13,s_ytd);
			stockValue.add(14,s_order_cnt);
			stockValue.add(15,s_remote_cnt);
			stockValue.add(16,s_data);

			String key = "9,w" + warehouse + "i" + i;
			cache.put(key, stockValue);
		}
	}
	
	// populate distrit table for a given warehouse
	public void initDistrict(int warehouse) {
		System.out.println("Populating district table");
		
		for (int i = 1; i <= NUM_DISTRICTS; i++) {
			String d_id = String.valueOf(i);
			String d_w_id = String.valueOf(warehouse);
			String d_name = generateAstring(6, 10);
			String d_street_1 = generateAstring(10, 20);
			String d_street_2 = generateAstring(10, 20);
			String d_city = generateAstring(10, 20);
			String d_state = generateLstring(2, 2);
			String d_zip = generateNstring(4, 4) + ZIP;
			String d_tax = String.valueOf(generateRandFloat(Float.valueOf("0.0000").floatValue(), Float.valueOf("0.2000").floatValue(), 4));
			String d_ytd = String.valueOf(Double.valueOf(300000.00).doubleValue());
			String d_next_o_id = String.valueOf(3001);
			
			ArrayList<String> districtValue = new ArrayList<>();
			districtValue.add(0,d_id);
			districtValue.add(1,d_w_id);
			districtValue.add(2,d_name);
			districtValue.add(3,d_street_1);
			districtValue.add(4,d_street_2);
			districtValue.add(5,d_city);
			districtValue.add(6,d_state);
			districtValue.add(7,d_zip);
			districtValue.add(8,d_tax);
			districtValue.add(9,d_ytd);
			districtValue.add(10,d_next_o_id);
			
			String key = "2,w" + warehouse + "d" + i;
			cache.put(key, districtValue);
			
			System.out.println("Populating customer table");
			initCustomer(warehouse, i);
			System.out.println("Populating order table");
			initOrder(warehouse, i);
		}
	}
	
	// populate customer table given warehouse and district
	public void initCustomer(int warehouse, int district) {
		
		for(int i = 1; i <= NUM_CUSTOMERS; i++) {
			String c_w_id = String.valueOf(warehouse);
			String c_d_id = String.valueOf(district);
			String c_id = String.valueOf(i);
			String c_first = generateAstring(8, 16);
			String c_middle = "OE";
			String c_last = lastName((int) nonUniformRandom(seedC_last, A_C_LAST, 0, MAX_C_LAST));
			//String c_last = c_last();
			String c_street1 = generateAstring(10, 20);
			String c_street2 = generateAstring(10, 20);
			String c_city = generateAstring(10, 20);
			String c_state = generateLstring(2, 2);
			String c_zip = generateNstring(4, 4) + ZIP;
			String c_phone = generateNstring(16, 16);
			String c_since = new Date(System.currentTimeMillis()).toString();
			String c_credit = (generateRandNum(1, 10) == 1) ? "BC" : "GC";
			String c_credit_lim = String.valueOf("500000.0");
			String c_discount = String.valueOf(generateRandDouble(0., 0.5, 4));
			String c_balance = String.valueOf(-10.0);
			String c_ytd_payment = String.valueOf(10.0);
			String c_payment_cnt = String.valueOf(1);
			String c_delivery_cnt = String.valueOf(0);
			String c_data = generateAstring(300, 500);
			
			ArrayList<String> customerValue = new ArrayList<>();
			customerValue.add(0,c_w_id);
			customerValue.add(1,c_d_id);
			customerValue.add(2,c_id);
			customerValue.add(3,c_first);
			customerValue.add(4,c_middle);
			customerValue.add(5,c_last);
			customerValue.add(6,c_street1);
			customerValue.add(7,c_street2);
			customerValue.add(8,c_city);
			customerValue.add(9,c_state);
			customerValue.add(10,c_zip);
			customerValue.add(11,c_phone);
			customerValue.add(12,c_since);
			customerValue.add(13,c_credit);
			customerValue.add(14,c_credit_lim);
			customerValue.add(15,c_discount);
			customerValue.add(16,c_balance);
			customerValue.add(17,c_ytd_payment);
			customerValue.add(18,c_payment_cnt);
			customerValue.add(19,c_delivery_cnt);
			customerValue.add(20,c_data);
			customerValue.add(21,null);
			
			String key = "3,w" + warehouse + "d" + district + "c" + i;
			cache.put(key, customerValue);
			
			initHistory(i, warehouse, district);
		}
	}
	
	// populate history table given warehouse, district and customer
	public void initHistory(int customer, int warehouse, int district) {
		
		String h_c_id = String.valueOf(customer);
		String h_d_id = String.valueOf(district);
		String h_w_id = String.valueOf(warehouse);
		String h_date = new Date(System.currentTimeMillis()).toString();
		String h_amount = String.valueOf(10.00);
		String h_data = generateAstring(12, 24);
		
		ArrayList<String> historyValue = new ArrayList<>();
		historyValue.add(0,h_c_id);
		historyValue.add(1,h_d_id);
		historyValue.add(2,h_w_id);
		historyValue.add(3,h_date);
		historyValue.add(4,h_amount);
		historyValue.add(5,h_data);
		
		String key = "4,w" + warehouse + "d" + district + "h" + customer;
		cache.put(key, historyValue);
	}
	
	// populate order table given warehouse and district
	public void initOrder(int warehouse, int district) {
		
		this.newOrder = false;
		
		for(int i = 1; i <= NUM_ORDERS; i++) {
			int o_ol_cnt = generateRandNum(5, 15);
			Date date = new Date((new Date()).getTime());
			String o_id = String.valueOf(i);
			//String o_c_id = String.valueOf(randomSeqNumber(1, NUM_CUSTOMERS));
			String o_c_id = String.valueOf(generateRandNum(1,3000));
			String o_d_id = String.valueOf(district);
			String o_w_id = String.valueOf(warehouse);
			String o_entry_d = date.toString();
			String o_carrier_id = String.valueOf(i < LIMIT_ORDER ? generateRandNum(1,10) : 0);
			String o_all_local = String.valueOf(1);
			
			ArrayList<String> orderValue = new ArrayList<>();
			orderValue.add(0,o_id);
			orderValue.add(1,o_d_id);
			orderValue.add(2,o_w_id);
			orderValue.add(3,o_c_id);
			orderValue.add(4,o_entry_d);
			orderValue.add(5,o_carrier_id);
			orderValue.add(6,String.valueOf(o_ol_cnt));
			orderValue.add(7,o_all_local);
			
			String key = "6,w" + warehouse + "d" + district + "o" + i;
			cache.put(key, orderValue);
			
			initOrderLine(warehouse, district, i, o_ol_cnt, date);
			if (i >= LIMIT_ORDER) initNewOrder(warehouse, district, i);		
		}
	}
	
	// populate order line table given warehouse, district, order, ol_cnt, date
	public void initOrderLine(int warehouse, int district, int order, int ol_cnt, Date date) {
		
		for (int i = 1; i <= ol_cnt; i++) {
			double amount;
			String delivery_date;
			
			if (order >= LIMIT_ORDER) {
				amount = generateRandDouble(0.01, 9999.99, 2);
				delivery_date = null;
			}
			else {
				amount = 0.0;
				delivery_date = date.toString();
			}
			
			
			String ol_o_id = String.valueOf(order);
			String ol_d_id = String.valueOf(district);
			String ol_w_id = String.valueOf(warehouse);
			String ol_number = String.valueOf(i);
			String ol_i_id = String.valueOf(nonUniformRandom(getOL_I_ID(), A_OL_I_ID, 1L, MAX_ITEM));
			String ol_supply_w_id = String.valueOf(warehouse);
			String ol_delivery_d = delivery_date;
			String ol_quantity = String.valueOf(5);
			String ol_amount = String.valueOf(amount);
			String ol_dist_info = generateAstring(24, 24);
			
			ArrayList<String> orderLineValue = new ArrayList<>();
			orderLineValue.add(0,ol_o_id);
			orderLineValue.add(1,ol_d_id);
			orderLineValue.add(2,ol_w_id);
			orderLineValue.add(3,ol_number);
			orderLineValue.add(4,ol_i_id);
			orderLineValue.add(5,ol_supply_w_id);
			orderLineValue.add(6,ol_delivery_d);
			orderLineValue.add(7,ol_quantity);
			orderLineValue.add(8,ol_amount);
			orderLineValue.add(9,ol_dist_info);
			
			String key = "7,w" + warehouse + "d" + district + "o" + order + "l" + i;
			cache.put(key, orderLineValue);
		}
	}
	
	// populate new order given warehouse, district, order number
	public void initNewOrder(int warehouse, int district, int order) {
		
		String no_o_id = String.valueOf(order);
		String no_d_id = String.valueOf(district);
		String no_w_id = String.valueOf(warehouse);
		
		ArrayList<String> newOrderValue = new ArrayList<>();
		newOrderValue.add(0,no_o_id);
		newOrderValue.add(1,no_d_id);
		newOrderValue.add(2,no_w_id);
		
		String key = "5,w" + warehouse + "d" + district + "no" + order;
		cache.put(key, newOrderValue);
	}
	
	// generate random alphanumeric string with given length
	public String generateAstring(int min, int max) {
		return generateAstring(min, max, DEFAULT_RADIX);
	}
	
	public String generateAstring(int min, int max, int radix) {
		if (min > max) return null;
		
		String chain = "";
		String str = null;
		
		int length = max;
		
		if (min != max) length = generateRandNum(min, max);
		
		for (int i = 0; i < length; i++) {
			int ref = rand.nextInt(2);
			int minUnicode = unicode[ref][0];
			int maxUnicode = unicode[ref][1];
			int random = rand.nextInt(maxUnicode - minUnicode + 1) + minUnicode;
			
			char c = (char) (((byte) random));
			chain += c;
		}
		try {
			str = new String(chain.toString().getBytes(), "UTF-8");
		}
		catch (UnsupportedEncodingException e) {
			System.out.println("Error generating alpha numeric string");
		}
		return str;
	}
	
	// generate random alpha string with given length
	public String generateLstring(int min, int max) {
		return generateNLstring(min, max, DEFAULT_MINL, DEFAULT_MAXL, DEFAULT_RADIX);
	}
	
	// generate random numeric string with given length
	public String generateNstring(int min, int max) {
		return generateNLstring(min, max, DEFAULT_MINN, DEFAULT_MAXN, DEFAULT_RADIX);
	}
	
	// generate random numeric or alpha string with given length
	public String generateNLstring(int min, int max, int minC, int maxC, int radix) {
		if (min > max) return null;
		String chain = new String();
		int length = max;
		
		if (min != max) length = generateRandNum(min, max);
		
		for (int i = 0; i < length; i++) {
			int random = rand.nextInt(max - min + 1) + min;
			char c = (char) (((byte) random) & 0xff);
			chain += c;
		}
		return chain;
	}
	
	
	private String lastName(int num)
    {
        return nameTokens[num/100] + nameTokens[(num/10)%10] + nameTokens[num%10];
    }
	
	// generate random number between min and max
	public int generateRandNum(int min, int max) {
		// get random number in the interval
		return this.rand.nextInt(max - min + 1) + min;
	}
	
	// generate random long between min and max
	public long generateRandLong(long min, long max) {
		long random = rand.nextLong() % (max + 1);
		while (random < min) random += max - min;
		return random;
	}
	
	// generate random float between min and max
	public float generateRandFloat(float min, float max, int virg) {
		if (min > max || virg < 1) return 0;
		long pow = (long) Math.pow(10, virg);
		long amin = (long) (min * pow);
		long amax = (long) (max * pow);
		long random = (long) (rand.nextDouble() * (amax - amin) + amin);
		return (float) random/pow;
	}
	
	// generate random float between min and max
	public double generateRandDouble(double min, double max, int virg) {
		if (min >= max || virg < 1) return 0;
		long pow = (long) Math.pow(10, virg);
		long amin = (long) (min * pow);
		long amax = (long) (max * pow);
		long random = (long) (rand.nextDouble() * (amax - amin) + amin);
		return (double) random/pow;
	}
	
	// generate string data
	public String s_data() {
        String randomString = generateAstring(this.S_DATA_MINN, this.S_DATA_MAXN);
        if (generateRandNum(1, 10) == 1) {
        	long number = randomNumber(0, randomString.length()-8);
        	randomString = randomString.substring(0, (int)number)+ORIGINAL+randomString.substring((int)number+8, randomString.length());
        }
        return randomString;
    }
	
	// generate c last
	public String c_last() {
		String c_last = "";
        long number = nonUniformRandom(getC_LAST(), A_C_LAST, MIN_C_LAST, MAX_C_LAST);
        String randomString = String.valueOf(number);
        while (randomString.length() < 3) {
            randomString = "0"+randomString;
        }
        for (int i=0; i<3; i++) {
            c_last += C_LAST[Integer.parseInt(randomString.substring(i, i+1))];
        }
        return c_last;
	}
	
	public long getC_LAST() {
        if (POP_C_LAST == NULL_NUMBER) {
            POP_C_LAST = randomNumber(MIN_C_LAST, A_C_LAST);
        }
        return POP_C_LAST;
	}

    public long getC_ID() {
        if (POP_C_ID == NULL_NUMBER) {
            POP_C_ID = randomNumber(0, A_C_ID);
        }
        return POP_C_ID;
    }

    public long getOL_I_ID() {
        if (POP_OL_I_ID == NULL_NUMBER) {
            POP_OL_I_ID = randomNumber(0, A_OL_I_ID);
        }
        return POP_OL_I_ID;
    }
	
	// generate random long number
	public long randomNumber(long min, long max) {
		return (long) (randUniform.nextDouble() * (max - min + 1) + min);
	}
	
	// generate random double number
	public double doubleRandomNumber(long min, long max) {
		return randUniform.nextDouble() * (max - min + 1) + min;
	}
	
	// generate random number non uniform
	public long randomNumberForNonUniform(long min, long max) {
		return (long) (randNonUniform.nextDouble() * (max - min + 1) + min);
	}
	
	// generate non uniform random
	public long nonUniformRandom(long type, long x, long min, long max) {
		return (((randomNumberForNonUniform(0,x) | randomNumberForNonUniform(min, max)) + type) % (max - min + 1)) + min;
	}
	
	// generate sequential random number
	public int randomSeqNumber(int min, int max) {
		this.customerIds = new int[max + 1];
		if (!newOrder) {
			for (int i = min; i <= max; i++) {
				this.customerIds[i] = i + 1;
			}
			this.newOrder = true;
		}
		int random = 0;
		int seqNumber = 0;
		do {
			random = (int) nonUniformRandom(getC_ID(), A_C_ID, min, max);
			seqNumber = this.customerIds[random];
		} while (seqNumber == NULL_NUMBER);
		customerIds[random] = NULL_NUMBER;
		return seqNumber;
	}
	
	// method to sort two dimensional array
	private ArrayList<ArrayList<String>> sortArray(ArrayList<ArrayList<String>> customerList) {		
		Collections.sort(customerList, new Comparator<ArrayList<String>>() {
			@Override
			public int compare(ArrayList<String> o1, ArrayList<String> o2) {
				return o1.get(3).compareTo(o2.get(3));
			}
		});
		return customerList;
	}
	
	// experiment specific partitioning function
	public static String computePartition(String table, String key) {
		// first experiment is 1w2p
		if (experimentNo == 1) {
			// catalyst
			if (systemNo == 1) {
				// every table other than 9 is an input split
				if (table.equals("8")) {
					System.out.println("Table 8 in the wrong KV-store");
					return "0";
				}
				else if (table.equals("1")) {
					return "0";
				}
				else if (!table.equals("9")) {
					if (getIDfromParam(key, "d") <= 5)
						return "0";
					else 
						return "1";
				}
				// table 9 is a table split
				else {
					return "0";
				}
			}
			// schism
			else {
				if (key.contains("d")) {
					int d = getIDfromParam(key, "d");
					if (d <= 9) {
						if (d <= 8) {
							if (d <= 6) {
								if (d <= 3) {
									if (d <= 2) {
										if (d <= 1) {
											return "1";
										}
										else {
											return "0";
										}
									}
									else {
										return "1";
									}
								}
								else {
									return "0";
								}
							}
							else {
								return "1";
							}
						}
						else {
							return "0";
						}
					}
					else {
						return "1";
					}
				}
				else {
					// no features in key from weka, run random hashing
					int part = (int) (key.toString().charAt(0) % 2);
					return String.valueOf(part);
				}
			}
		}
		// second experiment is 2w2p
		else if (experimentNo == 2) {
			// catalyst
			if (systemNo == 1) {
				if (table.equals("8")) {
					System.out.println("Table 8 in the wrong KV-store");
					return "1";
				}
				else if (getIDfromParam(key, "w") == 1)
					return "0";
				else
					return "1";
			}
			else {
				return "0";
			}
		}
		else {
			return "1";
		}
    }
	
	// aux function to get integer until new char
	private static int getIDfromParam(String key, String param) {
		// String to store results
		String result = "";
		// String to read integer from
		String intValue = key.substring(key.indexOf(param) + 1);
		// Remove extra
		for (int i = 0; i < intValue.length(); i++) {
			if (Character.isDigit(intValue.charAt(i))) {
				result += intValue.charAt(i);
			}
			else {
				break;
			}
		}
		return Integer.parseInt(result);
	}
	
	// partitioning is based on the key
	// key format must be "table,key"
	// based on this format we can apply table splits or input based splits
	
	public static class KeyGrouper implements Grouper<String> {
		
		 @Override
	     public String computeGroup(String key, String group) {
			// Get the table identifier from the key
			String table = key.split(",")[0];
			// Get the keyID from the key
			String keyID = key.split(",")[1];
			// Consult experiment specific partitioning function
			String partition = computePartition(table, keyID);
			return partition;
	     }

	     @Override
	     public Class<String> getKeyType() {
	        return String.class;
	     }   
	}
}
