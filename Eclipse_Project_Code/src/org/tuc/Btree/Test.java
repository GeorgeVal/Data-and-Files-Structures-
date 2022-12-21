package org.tuc.Btree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

/**
 * Main class of the project testing our b+tree methods
 * @author gv
 *
 */
public class Test {
	
	private static final String NODE_STORAGE_FILENAME = "plh201_node.bin";	//name of node file
	private static final String DATA_STORAGE_FILENAME = "plh201_data.bin";  //name of data file
	private static final int N = 100000;
	
	
	/**
	 * Empties node and data file
	 * @throws IOException
	 */
	private static void ResetFiles() throws IOException {
		RandomAccessFile raf = new RandomAccessFile(NODE_STORAGE_FILENAME, "rw");
		raf.setLength(0);
		raf.close();
		raf = new RandomAccessFile(DATA_STORAGE_FILENAME, "rw");
		raf.setLength(0);
		raf.close();
	}
	
	/**
	 * Creates an array of N+20 unique keys 
	 * @return the array
	 */
	private static  int[] RandomKeysGenerator() { //Creates random unique keys ranging from 1 to 10^6
		
			int START_INT = 1;
			int END_INT = 1000000;
			int NO_OF_ELEMENTS = N+20;
			java.util.Random randomGenerator = new java.util.Random();
			int[] randomInts = randomGenerator.ints(START_INT, END_INT).distinct().limit(NO_OF_ELEMENTS).toArray();
			return randomInts;
		}
	
	/**
	 * Generates random key from 1 to 1000000
	 * @return random key
	 */
	private static int RandomKeyGenerator() {
		   Random rand = new Random(); 
		   
	       // Generate random integers in range 0 to 1000000
	       int rand_int = rand.nextInt(1000000); 
	       if (rand_int == 0) rand_int = rand_int+1;
	       
	       return rand_int;
	   }
	
	
	/**
	 * Inserts N random unique keys to tree
	 * @param tree
	 * @return 20 unique keys for later use (for the insertion of 20 more keys)
	 * @throws IOException
	 */
	private static int[] RandomInsert(BTree tree) throws IOException {
		
		int[] keys = RandomKeysGenerator(); //creating random unique key array
		int[] leftKeys = new int[20];
		for(int i=0; i<N; i++) {
			tree.insert(keys[i], new Data(keys[i],keys[i]+10, keys[i]+20, keys[i]+30 )); //inserting keys from array
			if(i % (N/10) == 0) {
				System.out.println("Storing : "+(i*100)/N+"%");
			}
		}
		System.out.println("Storing Complete");
		System.arraycopy(keys, N,leftKeys, 0, 20);
		return leftKeys;
	}
	
	/**
	 * Inserts 20 unique keys in the tree and returns the mean disk accesses
	 * @param tree
	 * @param keys
	 * @return mean disk accesses
	 * @throws IOException
	 */
	private static int InsertRest20(BTree tree, int[] keys) throws IOException {
		int diskAccesses = 0;
		int sum = 0;
		for(int i =0; i<keys.length; i++) {
			
			MultiCounter.resetCounter(1);
			tree.insert(keys[i], new Data(keys[i],keys[i]+20, keys[i]+40, keys[i]+60));
			diskAccesses = MultiCounter.getCount(1);
			sum = sum + diskAccesses;			
		}
		return sum/20;
		
	}
	
	/**
	 * Searches for a Random Key in the tree and returns the disk accesses done while searching
	 * @param tree
	 * @return disk accesses
	 * @throws IOException
	 */
	private static int RandomSearch(BTree tree) throws IOException {
		int key = RandomKeyGenerator();
		tree.search(key);
		return MultiCounter.getCount(1);
	}
	
	/**
	 * Searches for 20 random keys and returns mean disk accesses
	 * @param tree
	 * @return mean disk accesses
	 * @throws IOException
	 */
	private static int RandomSearch20(BTree tree) throws IOException {
		int sum=0;
		for(int i=0; i<20; i++) {
			MultiCounter.resetCounter(1);
			sum = sum + RandomSearch(tree);
		}
		return sum/20;

	}
	
	/**
	 * Deletes a random key from the tree while counting disk accesses
	 * @param tree
	 * @return disk accesses
	 * @throws IOException
	 */
	private static int RandomDelete(BTree tree) throws IOException {
		int key = RandomKeyGenerator();
		tree.delete(key);
		return MultiCounter.getCount(1);

	}
	
	/**
	 * Deletes 20 random keys from tree and return mean disk accesses
	 * @param tree
	 * @return mean disk access
	 * @throws IOException
	 */
	private static int RandomDelete20(BTree tree) throws IOException {
		int sum=0;
		for(int i=0; i<20; i++) {
			MultiCounter.resetCounter(1);
			sum = sum + RandomDelete(tree);
		}
		return sum/20;

	}
	
	/**
	 * Searches keys in a random range its length is specified by range and counts disk Accesses
	 * @param tree is the tree we are going to search keys
	 * @param range is he range of the keys
	 * @return disk Accesses
	 * @throws IOException 
	 */
	private static int RandomRange(BTree tree, int range) throws IOException {
		MultiCounter.resetCounter(1);
		Integer lowRange = RandomKeyGenerator();
		Integer highRange = lowRange + range;
		tree.findRange(lowRange, highRange);
		return MultiCounter.getCount(1);
		
	}
	
	

	public static void main(String[] args) throws IOException {
		// Demo with Data as data
		
		int[] leftKeys = new int[20];		//leftKeys is used to store 20 uniqueKeys from RandomInsert that we will add later to our tree
		
		ResetFiles();	//empties all files
		
		BTree tree = new BTree();
		
		System.out.println("Insert of N keys initiated please wait 5-10 mins until storing is complete");
		leftKeys = RandomInsert(tree);
		
		System.out.println("Mean disk accesses in 20 inserts: "+InsertRest20(tree,leftKeys));
		
		System.out.println("Mean disk accesses in 20 random searches: "+RandomSearch20(tree));
		
		System.out.println("Mean disk accesses in 20 random deletes: "+RandomDelete20(tree));
		
		System.out.println("Mean disk accesses in  random range search of range 10: "+RandomRange(tree, 10));
		
		System.out.println("Mean disk accesses in  random range search of range 1000: "+RandomRange(tree, 1000));




		
		
		
		
	}

}
