package thesis;


import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Random;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.group.Grouper;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

public class TPCC {
	
	// infinispan parameters
	private final EmbeddedCacheManager cacheManager;
	private Cache<String, ArrayList<String>> cache;
	private final ClusterListener listener;
	
	// system parameters
	private static int experimentNo;
	private static int systemNo;
	
	// TPCC parameters
	private final int NUM_WAREHOUSES = 10;
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

	
	// build a cache for TPCC execution
	public TPCC(int experimentNo, int systemNo) {
		
		// set experiment number and systemNo
		TPCC.experimentNo = experimentNo;
		TPCC.systemNo = systemNo;
		
		GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
		// TODO Might need serialization here
		this.cacheManager = new DefaultCacheManager(global.build());
		listener = new ClusterListener(2);
	    cacheManager.addListener(listener);
		ConfigurationBuilder config = new ConfigurationBuilder();
		config.clustering().cacheMode(CacheMode.DIST_SYNC)
			.hash().numOwners(1).groups().enabled().addGrouper(new TPCC.KeyGrouper());
		this.cacheManager.defineConfiguration("tpcc", config.build());
		this.cache = cacheManager.getCache("tpcc");
		initCache();
		System.out.println("---- Waiting for cluster to form ----");
		try {
			listener.clusterFormedLatch.await();
		} catch (InterruptedException e) {
			System.out.println("Error while waiting for cluster to form");
			e.printStackTrace();
		}
		
	}
	
	// payment transaction
	private void paymentTransaction(long w_id, long c_w_id, double h_amount, long d_id, long c_d_id, long c_id, String c_last, boolean c_by_name) {
		// select the warehouse
		String warehouseKey = "1,w" + w_id;
		ArrayList<String> warehouse = cache.get(warehouseKey);
		// increase the ytd amount
		double amountYtd = Double.valueOf(warehouse.get(8)) + h_amount;
		warehouse.add(9, String.valueOf(amountYtd));
		// select the district
		String districtKey = "2,w" + w_id + "d" + d_id;
		ArrayList<String> district = cache.get(districtKey);
		// increase the ytd amount
		double amountDYtd = Double.valueOf(district.get(9)) + h_amount;
		district.add(10, String.valueOf(amountDYtd));
		
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
				int numSelected = (customerList.size() % 2 == 1 ? customerList.size() + 1 : customerList.size());
				// perform operations
				for (int i = 0; i < numSelected; i++) {
					// retrieve the customer
					ArrayList<String> selCustomer = customerList.get(numSelected);
					// decrease c_balance by h_amount
					double newBalance = Double.valueOf(selCustomer.get(16)) - h_amount;
					selCustomer.add(16, String.valueOf(newBalance));
					// increase c_ytd_payment by h_amount
					double newYtdPayment = Double.valueOf(selCustomer.get(17)) + h_amount;
					selCustomer.add(17, String.valueOf(newYtdPayment));
					// increase c_payment_cnt by 1
					int newCPaymentCnt = Integer.valueOf(selCustomer.get(18)) + 1;
					selCustomer.add(18, String.valueOf(newCPaymentCnt));
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
						selCustomer.add(20, c_new_data);
					}
					// update history
					String w_name = cache.get("1,w" + w_id).get(1);
					String d_name = cache.get("2,w" + w_id + "d" + d_id).get(2);
					String h_data = w_name + "    " + d_name;
					String historyKey = "4,w" + c_w_id + "d" + c_d_id + "h" + selCustomer.get(2);
					ArrayList<String> historyValue = cache.get(historyKey);
					historyValue.add(5, h_data);
					cache.put(historyKey, historyValue);
					// write the results to the db
					String key = "3,w" + c_w_id + "d" + c_d_id + "c" + selCustomer.get(2);
					cache.put(key, selCustomer);
				}
			}
		}
		else {
			// retrieve the customer
			String key = "3,w" + c_w_id + "d" + c_d_id + "c" + c_id;
			ArrayList<String> selCustomer = cache.get(key);
			// decrease c_balance by h_amount
			double newBalance = Double.valueOf(selCustomer.get(16)) - h_amount;
			selCustomer.add(16, String.valueOf(newBalance));
			// increase c_ytd_payment by h_amount
			double newYtdPayment = Double.valueOf(selCustomer.get(17)) + h_amount;
			selCustomer.add(17, String.valueOf(newYtdPayment));
			// increase c_payment_cnt by 1
			int newCPaymentCnt = Integer.valueOf(selCustomer.get(18)) + 1;
			selCustomer.add(18, String.valueOf(newCPaymentCnt));
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
				selCustomer.add(20, c_new_data);
			}
			// update history
			String w_name = cache.get("1,w" + w_id).get(1);
			String d_name = cache.get("2,w" + w_id + "d" + d_id).get(2);
			String h_data = w_name + "    " + d_name;
			String historyKey = "4,w" + c_w_id + "d" + c_d_id + "h" + selCustomer.get(2);
			ArrayList<String> historyValue = cache.get(historyKey);
			historyValue.add(5, h_data);
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
		// increment next order id for district
		int nextOrder = Integer.valueOf(districtValue.get(10));
		districtValue.add(10, String.valueOf(nextOrder + 1));
		cache.put(districtKey, districtValue);
		// retrieve the customer key
		String customerKey = "3,w" + w_id + "d" + d_id + "c" + c_id;
		cache.get(customerKey);
		// insert in new order 
		String newOrderKey = "5,w" + w_id + "d" + d_id + "no" + nextOrder;
		ArrayList<String> newOrderValue = cache.get(newOrderKey);
		cache.put(newOrderKey, newOrderValue);
		// insert in order
		String orderKey = "6,w" + w_id + "d" + d_id + "o" + nextOrder;
		ArrayList<String> orderValue = cache.get(orderKey);
		orderValue.add(5, String.valueOf(-1));
		orderValue.add(7, String.valueOf(o_all_local));
		cache.put(orderKey, orderValue);
		// for each item in the ordder
		for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
			// array reads for auxiliary values
			long ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
			long ol_i_id = itemIDs[ol_number - 1];
			long ol_quantity = orderQuantities[ol_number - 1];
			
			// item table read
			String itemKey = "8,i" + ol_i_id;
			ArrayList<String> itemValue = cache.get(itemKey);
			
			// stock table 
			String stockKey = "9,w" + ol_supply_w_id + "i" + ol_i_id;
			ArrayList<String> stockValue = cache.get(stockKey);
			if (Long.valueOf(cache.get(stockKey).get(2)) - ol_quantity >= 10) {
				long newQuantity = Long.valueOf(cache.get(stockKey).get(2)) - ol_quantity;
				stockValue.add(2, String.valueOf(newQuantity));
			}
			else {
				long newQuantity = (Long.valueOf(cache.get(stockKey).get(2)) - ol_quantity) + 91;
				stockValue.add(2, String.valueOf(newQuantity));
			}
			
			int s_remote_cnt_increment;
			// check if order line is to a remote warehouse
			if (ol_supply_w_id == w_id) {
				s_remote_cnt_increment = 0;
			}
			else {
				s_remote_cnt_increment = 1;
			}
			long newYtd = Long.valueOf(cache.get(stockKey).get(13)) + ol_quantity;
			stockValue.add(13, String.valueOf(newYtd));
			int s_order_cntNew = Integer.valueOf(stockValue.get(14)) + 1;
			stockValue.add(14, String.valueOf(s_order_cntNew));
			int s_remote_cntNew = Integer.valueOf(stockValue.get(15)) + s_remote_cnt_increment;
			stockValue.add(15, String.valueOf(s_remote_cntNew));
			cache.put(stockKey, stockValue);
			
			float ol_amount = ol_quantity * Float.valueOf(itemValue.get(3));
			
			// orderline insert
			String orderLineKey = "7,w" + w_id + "d" + d_id + "o" + nextOrder + "l" + ol_number;
			String ol_dist_info = stockValue.get((int) d_id + 2);
			ArrayList<String> orderLineValue = cache.get(orderLineKey);
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
			cache.put(orderLineKey, orderLineValue);
		}
		
	}
	
	// order status transaction
	
	
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
			itemValue.add(i_id);
			itemValue.add(i_im_id);
			itemValue.add(i_name);
			itemValue.add(i_price);
			itemValue.add(i_data);
			
			String key = "8,i" + i;
			cache.put(key, itemValue);
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
			warehouseValue.add(w_id);
			warehouseValue.add(w_name);
			warehouseValue.add(w_street_1);
			warehouseValue.add(w_street_2);
			warehouseValue.add(w_city);
			warehouseValue.add(w_state);
			warehouseValue.add(w_zip);
			warehouseValue.add(w_tax);
			warehouseValue.add(w_ytd);
			
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
			stockValue.add(s_i_id);
			stockValue.add(s_w_id);
			stockValue.add(s_quantity);
			stockValue.add(s_dist_01);
			stockValue.add(s_dist_02);
			stockValue.add(s_dist_03);
			stockValue.add(s_dist_04);
			stockValue.add(s_dist_05);
			stockValue.add(s_dist_06);
			stockValue.add(s_dist_07);
			stockValue.add(s_dist_08);
			stockValue.add(s_dist_09);
			stockValue.add(s_dist_10);
			stockValue.add(s_ytd);
			stockValue.add(s_order_cnt);
			stockValue.add(s_remote_cnt);
			stockValue.add(s_data);

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
			districtValue.add(d_id);
			districtValue.add(d_w_id);
			districtValue.add(d_name);
			districtValue.add(d_street_1);
			districtValue.add(d_street_2);
			districtValue.add(d_city);
			districtValue.add(d_state);
			districtValue.add(d_zip);
			districtValue.add(d_tax);
			districtValue.add(d_ytd);
			districtValue.add(d_next_o_id);
			
			String key = "2,w" + warehouse + "d" + i;
			cache.put(key, districtValue);
			
			initCustomer(warehouse, i);
			initOrder(warehouse, i);
		}
	}
	
	// populate customer table given warehouse and district
	public void initCustomer(int warehouse, int district) {
		System.out.println("Populating customer table");
		
		for(int i = 1; i <= NUM_CUSTOMERS; i++) {
			String c_w_id = String.valueOf(warehouse);
			String c_d_id = String.valueOf(district);
			String c_id = String.valueOf(i);
			String c_first = generateAstring(8, 16);
			String c_middle = "OE";
			String c_last = c_last();
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
			customerValue.add(c_w_id);
			customerValue.add(c_d_id);
			customerValue.add(c_id);
			customerValue.add(c_first);
			customerValue.add(c_middle);
			customerValue.add(c_last);
			customerValue.add(c_street1);
			customerValue.add(c_street2);
			customerValue.add(c_city);
			customerValue.add(c_state);
			customerValue.add(c_zip);
			customerValue.add(c_phone);
			customerValue.add(c_since);
			customerValue.add(c_credit);
			customerValue.add(c_credit_lim);
			customerValue.add(c_discount);
			customerValue.add(c_balance);
			customerValue.add(c_ytd_payment);
			customerValue.add(c_payment_cnt);
			customerValue.add(c_delivery_cnt);
			customerValue.add(c_data);
			
			String key = "3,w" + warehouse + "d" + district + "c" + i;
			cache.put(key, customerValue);
			
			initHistory(i, warehouse, district);
		}
	}
	
	// populate history table given warehouse, district and customer
	public void initHistory(int customer, int warehouse, int district) {
		System.out.println("Populating history table");
		
		String h_c_id = String.valueOf(customer);
		String h_d_id = String.valueOf(district);
		String h_w_id = String.valueOf(warehouse);
		String h_date = new Date(System.currentTimeMillis()).toString();
		String h_amount = String.valueOf(10.00);
		String h_data = generateAstring(12, 24);
		
		ArrayList<String> historyValue = new ArrayList<>();
		historyValue.add(h_c_id);
		historyValue.add(h_d_id);
		historyValue.add(h_w_id);
		historyValue.add(h_date);
		historyValue.add(h_amount);
		historyValue.add(h_data);
		
		String key = "4,w" + warehouse + "d" + district + "h" + customer;
		cache.put(key, historyValue);
	}
	
	// populate order table given warehouse and district
	public void initOrder(int warehouse, int district) {
		System.out.println("Populating order table");
		
		this.newOrder = false;
		
		for(int i = 1; i <= NUM_ORDERS; i++) {
			int o_ol_cnt = generateRandNum(5, 15);
			Date date = new Date((new Date()).getTime());
			String o_id = String.valueOf(i);
			String o_c_id = String.valueOf(randomSeqNumber(0, NUM_CUSTOMERS - 1));
			String o_d_id = String.valueOf(district);
			String o_w_id = String.valueOf(warehouse);
			String o_entry_d = date.toString();
			String o_carrier_id = String.valueOf(i < LIMIT_ORDER ? generateRandNum(1,10) : 0);
			String o_all_local = String.valueOf(1);
			
			ArrayList<String> orderValue = new ArrayList<>();
			orderValue.add(o_id);
			orderValue.add(o_d_id);
			orderValue.add(o_w_id);
			orderValue.add(o_c_id);
			orderValue.add(o_entry_d);
			orderValue.add(o_carrier_id);
			orderValue.add(String.valueOf(o_ol_cnt));
			orderValue.add(o_all_local);
			
			String key = "6,w" + warehouse + "d" + district + "o" + i;
			cache.put(key, orderValue);
			
			initOrderLine(warehouse, district, i, o_ol_cnt, date);
			if (i >= LIMIT_ORDER) initNewOrder(warehouse, district, i);		
		}
	}
	
	// populate order line table given warehouse, district, order, ol_cnt, date
	public void initOrderLine(int warehouse, int district, int order, int ol_cnt, Date date) {
		System.out.println("Populating order line table");
		
		for (int i = 0; i < ol_cnt; i++) {
			double amount;
			Date delivery_date;
			
			if (order >= LIMIT_ORDER) {
				amount = generateRandDouble(0.01, 9999.99, 2);
				delivery_date = null;
			}
			else {
				amount = 0.0;
				delivery_date = date;
			}
			
			String ol_o_id = String.valueOf(order);
			String ol_d_id = String.valueOf(district);
			String ol_w_id = String.valueOf(warehouse);
			String ol_number = String.valueOf(i);
			String ol_i_id = String.valueOf(nonUniformRandom(getOL_I_ID(), A_OL_I_ID, 1L, MAX_ITEM));
			String ol_supply_w_id = String.valueOf(warehouse);
			String ol_delivery_d = delivery_date.toString();
			String ol_quantity = String.valueOf(5);
			String ol_amount = String.valueOf(amount);
			String ol_dist_info = generateAstring(24, 24);
			
			ArrayList<String> orderLineValue = new ArrayList<>();
			orderLineValue.add(ol_o_id);
			orderLineValue.add(ol_d_id);
			orderLineValue.add(ol_w_id);
			orderLineValue.add(ol_number);
			orderLineValue.add(ol_i_id);
			orderLineValue.add(ol_supply_w_id);
			orderLineValue.add(ol_delivery_d);
			orderLineValue.add(ol_quantity);
			orderLineValue.add(ol_amount);
			orderLineValue.add(ol_dist_info);
			
			String key = "7,w" + warehouse + "d" + district + "o" + order + "l" + i;
			cache.put(key, orderLineValue);
		}
	}
	
	// populate new order given warehouse, district, order number
	public void initNewOrder(int warehouse, int district, int order) {
		System.out.println("Populating new order table");
		
		String no_o_id = String.valueOf(order);
		String no_d_id = String.valueOf(district);
		String no_w_id = String.valueOf(warehouse);
		
		ArrayList<String> newOrderValue = new ArrayList<>();
		newOrderValue.add(no_o_id);
		newOrderValue.add(no_d_id);
		newOrderValue.add(no_w_id);
		
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
				if (!table.equals("9")) {
					if (getIDfromParam(key, "d") <= 4)
						return "1";
					else 
						return "0";
				}
				// table 9 is a table split
				else {
					return "0";
				}
			}
			else {
				return "0";
			}
		}
		else {
			return "0";
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
