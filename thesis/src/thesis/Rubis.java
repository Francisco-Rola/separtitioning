package thesis;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

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
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;

// TODO fix keys and look at logic
// user key could be rXuY
// item key could be cXiY
// comment key could be uXtY
// bid key could be iXdY
// buy key could be iXyY


public class Rubis {
	
	// general parameters
	private static int systemNo;
	private int nodeId;
	private static int experimentNo;
	
	//infinispan cache 
	private final EmbeddedCacheManager cacheManager;
	private final TransactionManager transactionManager;
	private Cache<String, ArrayList<String>> cache;
	private Cache<String, ArrayList<String>> replCache;
	private final ClusterListener listener;
	
	// rubis parameters
	private final int MAX_USERS = 1000000;
	private final int MAX_ITEMS = 100000;
	private final int MAX_COMMENTS = 100;
	private final int MAX_BIDS = 1000;
	private final int MAX_BUYS = 100;
	
	

	// build a cache for Rubis execution
	public Rubis(int systemNo, int experimentNo, int nodeId) {		
		// set node Id
		this.nodeId = nodeId;		
		// set experiment number and systemNo
		Rubis.experimentNo = experimentNo;
		Rubis.systemNo = systemNo;
		
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
		configReplicated.locking()
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
			    .recoveryInfoCacheName("__recoveryInfoCacheName__"); 
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
			// rubis experiment has 2 nodes
			if (experimentNo == 7) {
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
			else 
				break;
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

	private void initCache() {
		
	}
	
	private void executeTransaction() throws NotSupportedException, SystemException, SecurityException, IllegalStateException, RollbackException, HeuristicMixedException, HeuristicRollbackException {
		
	}
	
	// store buy transaction logic
	private void storeBuy(int userId, int itemId, int maxQty, int qty) {
		// retrieve item information
		String itemKey = "2,i" + itemId;
		ArrayList<String> itemValue = cache.get(itemKey);
		if (itemValue == null) {
			System.out.println("Store buy: item key not found");
		}
		long endTime = Long.valueOf(itemValue.get(10));
		int quantity = Integer.valueOf(itemValue.get(4));
		int boughtItems = Integer.valueOf(itemValue.get(13));
		
		long timeNow = Long.valueOf(System.currentTimeMillis());
		quantity -= qty;
		
		// update bought items in db
		itemValue.set(13, String.valueOf(boughtItems + 1));
		
		int buyId = getBuyId(itemId, boughtItems);
		
		if (quantity == 0) {
			itemValue.set(4, String.valueOf(quantity));
			itemValue.set(10, String.valueOf(timeNow));
		}
		else {
			itemValue.set(4, String.valueOf(quantity));
		}
		cache.put(itemKey, itemValue);
		
		// create a buy value
		ArrayList<String> buyValue = new ArrayList<>();
		buyValue.add(String.valueOf(buyId));
		buyValue.add(String.valueOf(userId));
		buyValue.add(String.valueOf(itemId));
		buyValue.add(String.valueOf(qty));
		buyValue.add(String.valueOf(timeNow));
		
		//write buy to db
		String buyKey = "6,y" + buyId;
		cache.put(buyKey, buyValue);
	}
	
	
	// store bid transaction logic
	private void storeBid(int userId, int itemId, int minBid, int bid, int maxBid, int maxQty, int qty) {
		// increment number of bids for item
		String itemKey = "2,i" + itemId;
		ArrayList<String> itemValue = cache.get(itemKey);
		if (itemValue == null) {
			System.out.println("Store Bid: item key not found!");
		}
		int numBidsForItem = Integer.valueOf(itemValue.get(7));
		itemValue.set(7, String.valueOf(numBidsForItem + 1));
		// write update to db
		cache.put(itemKey, itemValue);
		// get a bid id
		int bidId = getBidId(itemId, numBidsForItem);
		// create a bid value
		ArrayList<String> bidValue = new ArrayList<>();
		bidValue.add(String.valueOf(bidId));
		bidValue.add(String.valueOf(userId));
		bidValue.add(String.valueOf(qty));
		bidValue.add(String.valueOf(bid));
		bidValue.add(String.valueOf(maxBid));
		bidValue.add(String.valueOf(System.currentTimeMillis()));
		// create a bid key
		String bidKey = "5,d" + bidId;
		// write value to db
		cache.put(bidKey, bidValue);
		
		// store the bid in table 9
		if (numBidsForItem == 0) {
			String bidListKey = "9,i" + itemId;
			ArrayList<String> bidListValue = new ArrayList<>();
			bidListValue.add(String.valueOf(itemId));
			bidListValue.add(String.valueOf(1));
			bidListValue.add(String.valueOf(bidId));
			cache.put(bidListKey, bidListValue);
		}
		else {
			String bidListKey = "9,i" + itemId;
			ArrayList<String> bidListValue = cache.get(bidListKey);
			if (bidListValue == null) {
				System.out.println("Store bid: bid list key not found!");
			}
			bidListValue.set(1, String.valueOf(numBidsForItem + 1));
			bidListValue.add(String.valueOf(bidId));
			cache.put(bidListKey, bidListValue);
		}
	}
	
	// store comment transaction logic
	private void storeComment(int toId, int fromId, int itemId, int comment, int rating) {
		// get comments by the user
		String commentFromKey = "1,u" + fromId;
		ArrayList<String> commentFromValue = cache.get(commentFromKey);
		if (commentFromValue == null) {
			System.out.println("Store Comment: from user key not found!");
		}
		// retrieve number of comments by user
		int commentsByUser = Integer.valueOf(commentFromValue.get(10));
		// generate the next available commentId
		int commentId = getCommentId(fromId, commentsByUser);
		// update the number of comments by the user in db
		commentFromValue.set(10, String.valueOf(commentsByUser + 1));
		cache.put(commentFromKey, commentFromValue);
		// create the comment value
		ArrayList<String> commentValue = new ArrayList<String>();
		// write to the comment value
		commentValue.add(String.valueOf(commentId));
		commentValue.add(String.valueOf(fromId));
		commentValue.add(String.valueOf(toId));
		commentValue.add(String.valueOf(itemId));
		commentValue.add(String.valueOf(rating));
		commentValue.add(String.valueOf(System.currentTimeMillis()));
		commentValue.add(String.valueOf(comment));
		// create a comment key
		String commentKey = "7,t" + commentId;
		// write comment value to db
		cache.put(commentKey, commentValue);
		
		// find the target user of the comment and retrieve seller rating
		String commentToKey = "1,u" + toId;
		ArrayList<String> commentToValue = cache.get(commentToKey);
		if (commentToValue == null) {
			System.out.println("Store Comment: to user key not found!");
		}
		// retriever seller rating
		int sellerRating = Integer.valueOf(commentToValue.get(6));
		// update seller rating
		commentToValue.set(6, String.valueOf(sellerRating + rating));
		// write update to the db
		cache.put(commentToKey, commentToValue);
		
	}
	
	// register user transaction logic
	private void registerUser(int firstName, int lastName, int nickname, int password, int email, int regionId) {
		// read the number of user in region
		String regionKey = "4,r" + regionId;
		ArrayList<String> regionValue = cache.get(regionKey);
		if (regionValue == null) {
			System.out.println("Register User: region key not found!");
		}
		// retrieve the number of user in region
		int usersInRegion = Integer.valueOf(regionValue.get(2));
		// generate the next available userId
		int userId = getUserId(regionId, usersInRegion);
		// update number of users in region in db
		regionValue.set(2, String.valueOf(usersInRegion + 1));
		cache.put(regionKey, regionValue);
		// create the user value
		ArrayList<String> userValue = new ArrayList<String>();
		// write to the userValue
		userValue.add(String.valueOf(userId));
		userValue.add(String.valueOf(firstName));
		userValue.add(String.valueOf(lastName));
		userValue.add(String.valueOf(nickname));
		userValue.add(String.valueOf(password));
		userValue.add(String.valueOf(email));
		userValue.add(String.valueOf(0));
		userValue.add(String.valueOf(0));
		userValue.add(String.valueOf(System.currentTimeMillis()));
		userValue.add(String.valueOf(0));
		userValue.add(String.valueOf(0));
		// create a user key
		String userKey = "1,u" + userId;
		// write user value to db
		cache.put(userKey, userValue);
	}
	
	// register item transaction logic
	private void registerItem(int name, int description, int initialPrice, int buyNow, int reservePrice, int quantity, int duration, int categoryId, int userId) {
		// read the number of items in category
		String categoryKey = "3,c" + categoryId;
		ArrayList<String> categoryValue = cache.get(categoryKey);
		if (categoryValue == null) {
			System.out.println("Register Item: category key not found!");
		}
		// retrieve number of items in category
		int itemsInCategory = Integer.valueOf(categoryValue.get(2));
		// generate the next available itemId
		int itemId = getItemId(categoryId, itemsInCategory);
		// update number of items in category in db
		categoryValue.set(2, String.valueOf(itemsInCategory + 1));
		cache.put(categoryKey, categoryValue);
		// create the item value
		ArrayList<String> itemValue = new ArrayList<String>();
		// write to the itemValue
		itemValue.add(String.valueOf(itemId));
		itemValue.add(String.valueOf(name));
		itemValue.add(String.valueOf(description));
		itemValue.add(String.valueOf(initialPrice));
		itemValue.add(String.valueOf(quantity));
		itemValue.add(String.valueOf(reservePrice));
		itemValue.add(String.valueOf(buyNow));
		itemValue.add(String.valueOf(0));
		itemValue.add(String.valueOf(0));
		itemValue.add(String.valueOf(System.currentTimeMillis()));
		itemValue.add(String.valueOf(System.currentTimeMillis() + 300000));
		itemValue.add(String.valueOf(userId));
		itemValue.add(String.valueOf(categoryId));
		itemValue.add(String.valueOf(0));
		// create item Key
		String itemKey = "2,i" + itemId;
		// write item to db
		cache.put(itemKey, itemValue);
	}
	
	// create a new item id based on category id and number of items allowed per category
	private int getItemId(int category, int itemsInCategory) {
		// report an error if more than limit items in category as there will be collisions
		if (itemsInCategory > MAX_ITEMS - 1) {
			System.out.println("Category is overflown, results will be incorrect!");
		}
		// category supports up to MAX items
		int itemId = category * MAX_ITEMS + itemsInCategory;
		return itemId;
	}
	
	// create a new user id based on region id and number of users allowed per region
	private int getUserId(int region, int usersInRegion) {
		// report an error if more than limit users in region as there will be collisions
		if (usersInRegion > MAX_USERS - 1) {
			System.out.println("Region is overflown, results will be incorrect!");
		}
		// category supports up to MAX users
		int userId = region * MAX_USERS + usersInRegion;
		return userId;
	}
	
	// create a new comment id based on user and number of comments allowed per user
	private int getCommentId(int user, int numCommentsByUser) {
		// report an error if more than limit of comments as there will be collisions
		if (numCommentsByUser > MAX_COMMENTS - 1) {
			System.out.println("Comments is overflown, results will be incorrect!");
		}
		// comments supports up to MAX comments
		int commentID = user * MAX_COMMENTS + numCommentsByUser;
		return commentID;
	}
	
	// create a new bid id based on item Id and number of bids allowed per item
	private int getBidId(int item, int numBidsOnItem) {
		// report an error if more than limit of bids as there will be collisions
		if (numBidsOnItem > MAX_BIDS - 1) {
			System.out.println("Bids is overflown, results will be incorrect!");
		}
		// bids supports up to MAX Bids
		int bidId = item * MAX_BIDS + numBidsOnItem;
		return bidId;
	}
	
	// create a new buy now id based on item id and number of buy now allowed per item
	private int getBuyId(int item, int numBuysOnItem) {
		// report an error if more than limit of buys as there will be collisions
		if (numBuysOnItem > MAX_BUYS - 1) {
			System.out.println("Buys is overflown, results will be incorrect!");
		}
		// buys supports up to MAX Buys
		int buyId = item * MAX_BUYS + numBuysOnItem;
		return buyId;
	}
}
