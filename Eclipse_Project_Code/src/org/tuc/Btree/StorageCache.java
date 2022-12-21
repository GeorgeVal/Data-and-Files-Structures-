package org.tuc.Btree;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

/**
 * Basic singleton handling retrieving and storing BTree Nodes to node/index file and Data to data file.
 * @author gv
 *
 */
public class StorageCache {
	private static final String NODE_STORAGE_FILENAME = "plh201_node.bin";
	private static final String DATA_STORAGE_FILENAME = "plh201_data.bin";
	private static final int PageSize = 256;
	private static final int DataSize = 32;
	//private static final int DataPageSize = 32;
	
	private static StorageCache instance;
	
	@SuppressWarnings("rawtypes")
	private static HashMap retrievedNodes = null;
	@SuppressWarnings("rawtypes")
	private static HashMap retrievedDatas = null;
	
	// make this private so that no one can create instances of this class
	
	@SuppressWarnings("rawtypes")
	private StorageCache() {
		  retrievedNodes = new HashMap();
		  retrievedDatas = new HashMap();
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void cacheNode(int dataPageIndex, BTreeNode node) {
		if (StorageCache.retrievedNodes == null) {
			StorageCache.retrievedNodes = new HashMap();
		}
		StorageCache.retrievedNodes.put(dataPageIndex, node);
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void cacheData(int dataByteOffset, Data data) {
		if (StorageCache.retrievedDatas == null) {
			StorageCache.retrievedDatas = new HashMap();
		}
		StorageCache.retrievedDatas.put(dataByteOffset, data);
	}
	
	private BTreeNode getNodeFromCache(int dataPageIndex) {
		if (StorageCache.retrievedNodes == null) {
			return null;
		}
		
		return (BTreeNode)StorageCache.retrievedNodes.get(dataPageIndex);
	}
	private Data getDataFromCache(int dataByteOffset) {
		if (StorageCache.retrievedDatas == null) {
			return null;
		}
		
		return (Data)StorageCache.retrievedDatas.get(dataByteOffset);
	}	
	
	public static StorageCache getInstance() {
		if (StorageCache.instance == null) {
			StorageCache.instance = new StorageCache();
		}
		return StorageCache.instance;
	}
	
	public void flush() throws IOException {
		flushNodes();
		flushData();
	}
	
	// checks each node in retrievedNodes whether it is dirty
	// If they are dirty, writes them to disk
	@SuppressWarnings("rawtypes")
	private void flushNodes() throws IOException {
		int pos;
		BTreeNode node;
		for ( Object dataPageIndex : StorageCache.retrievedNodes.keySet() ) {
			node = (BTreeNode)StorageCache.retrievedNodes.get(dataPageIndex);
			if (node.isDirty()) {
				byte[] byteArray = node.toByteArray();
				try {
					// store byteArray to node/index file at byte position dataPageIndex * DATA_PAGE_SIZE
					RandomAccessFile raf = new RandomAccessFile(NODE_STORAGE_FILENAME, "rw");
					pos=(int)dataPageIndex*PageSize;
					raf.seek(pos);
					raf.write(byteArray);
					raf.close();
					
				}
				catch (IOException e) {
					 System.err.println("Write failed in flushNodes");
					 
				}
				// ******************************
				// we just wrote a data page to our file. This is a good location to increase our counter!!!!!
				// ******************************
				MultiCounter.increaseCounter(1);
			}
		}
		
		// reset it
		StorageCache.retrievedNodes = new HashMap();	
	}
	
	
	@SuppressWarnings("rawtypes")
	private void flushData() throws IOException {
		Data data;
		int dataPageIndex;
		
		
		for ( Object storageByteOffset : StorageCache.retrievedDatas.keySet() ) {
			data = (Data)StorageCache.retrievedDatas.get(storageByteOffset);
			if (data.isDirty()) {
				// data.storageByteIndex tells us at which byte offset in the data file this data is stored
				// From this value, and knowing our data page size, we can calculate the dataPageIndex of the data page in the data file
				// This process may result in writing each data page multiple times if it contains multiple dirty Datas
				byte[] buffer = new byte[PageSize];
				byte[] byteArray = data.toByteArray();
				dataPageIndex = data.getStorageByteOffset()/PageSize;
				try {
					RandomAccessFile raf = new RandomAccessFile(DATA_STORAGE_FILENAME, "rw");
					// read datapage given by calculated dataPageIndex from data file
					// copy byteArray to correct position of read bytes
					// store it again to file
					// ......
					// ......
					// ......
					raf.seek(dataPageIndex*PageSize);
					raf.read(buffer);
					System.arraycopy(byteArray, 0, buffer, data.getStorageByteOffset()-dataPageIndex*PageSize, DataSize);
					raf.seek(dataPageIndex*PageSize);
					raf.write(buffer);
					raf.close();
					
				}
				catch (IOException e) {
					 System.err.println("Write failed in flushNodes");
				}
				
			
				// ******************************
				// we just wrote a data page to our file. This is a good location to increase our counter!!!!!
				// ******************************
				MultiCounter.increaseCounter(1,2);
			}
		}
		
		// reset it
		StorageCache.retrievedDatas = new HashMap();	
	}
	

	public BTreeNode retrieveNode(int dataPageIndex) throws IOException {
		// if we have this dataPageIndex already in the cache, return it
		BTreeNode result = this.getNodeFromCache(dataPageIndex);
		if (result != null) {
			return result;
		}
		
		// OPTIONAL, not important for this assignment
		// during a range search, we will potentially retrieve a large set of nodes, despite we will use them only once
		// We can optionally add here a case where "large" number of cached, NOT DIRTY (!) nodes, are removed from memory
		if (StorageCache.retrievedNodes != null && StorageCache.retrievedNodes.keySet().size() > 100) { // we do not want to have more than 100 nodes in cache
			BTreeNode node;
			for ( Object key : StorageCache.retrievedNodes.keySet() ) {
				node = (BTreeNode)StorageCache.retrievedNodes.get(dataPageIndex);
				if (!node.isDirty()) {
					StorageCache.retrievedNodes.remove(key);
				}
			}
		}
		
		byte[] byteArray = new byte[PageSize];
		// open our node/index file
		RandomAccessFile raf = new RandomAccessFile(NODE_STORAGE_FILENAME, "rw");
		// seek to position DATA_PAGE_SIZE * dataPageIndex
		int pos = PageSize*dataPageIndex;
		//System.out.println("position: "+pos);
		raf.seek(pos);
		// read DATA_PAGE_SIZE bytes (some constant)
		// byte[] pageBytes = raf.read .....;
		raf.read(byteArray);
		raf.close();
		// a 4 byte int should tell us what kind of node this is. See toByteArray(). Is it a BTreeInnerNode or a BTreeLeafNode?
				// int type = read the 4 byte
				
				// if type corresponds to inner node
				//     result = new BTreeInnerNode();
				//     result = result.fromByteArray(pageBytes, dataPageIndex);
				// else this is a leaf node
				//     result = new BTreeLeafNode();
				//     result = result.fromByteArray(pageBytes, dataPageIndex);
		
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
			DataInputStream ois = new DataInputStream(bis);
			
			int type = ois.readInt();
			if(type==0) {
				result = new BTreeInnerNode();
				result = result.fromByteArray(byteArray, dataPageIndex);
			}
			else if(type==1) {
				result = new BTreeLeafNode();
				result = result.fromByteArray(byteArray, dataPageIndex);
			}
			else {
				System.out.println("Problem in identifying type");
				return null;
			}   
			ois.close();
			bis.close();
			}
			catch (IOException e) {
				 System.err.println("Read or write failed in retrieveNode");
				 return null;
			}		
		// ******************************
		// we just read a data page from our file. This is a good location to increase our counter!!!!!
		// ******************************
		MultiCounter.increaseCounter(1);
		
		// before returning it, cache it for future reference
		this.cacheNode(dataPageIndex, result);
		
		
		return result;
		
	}
	
	
	
	public Data retrieveData(int dataByteOffset) throws IOException {
		// if we have this dataPageIndex already in the cache, return it
		Data result = this.getDataFromCache(dataByteOffset);
		if (result != null) {
			return result;
		}
		
		// OPTIONAL, not important for this assignment
		// during a range search, we will potentially retrieve a large set of datas, despite we will use them only once
		// We can optionally add here a case where "large" number of cached, NOT DIRTY (!) datas, are removed from memory
		if (StorageCache.retrievedDatas != null && StorageCache.retrievedDatas.keySet().size() > 100) { // we do not want to have more than 100 datas in cache
			Data data;
			for ( Object key : StorageCache.retrievedDatas.keySet() ) {
				data = (Data)StorageCache.retrievedDatas.get(dataByteOffset);
				if (!data.isDirty()) {
					StorageCache.retrievedDatas.remove(key);
				}
			}
		}
		byte[] byteArray = new byte[PageSize];
		byte[] fixByteArray = new byte[DataSize];
		RandomAccessFile raf = new RandomAccessFile(DATA_STORAGE_FILENAME,"rw" );
		// open our data file
		int pos = dataByteOffset/PageSize;
		// seek to position of the data page that corresponds to dataByteOffset
		raf.seek(pos*PageSize);
		// read DATA_PAGE_SIZE bytes (some constant)
		// byte[] pageBytes = raf.read .....;
		raf.read(byteArray);
		raf.close();
		// get the part of the bytes that corresponds to dataByteOffset (--> pageBytesData), and transform to a Data instance
		System.arraycopy(byteArray, dataByteOffset-pos*PageSize ,fixByteArray ,0 , DataSize);
		result = new Data();
		result = result.fromByteArray(fixByteArray, dataByteOffset);
		
		// ******************************
		// we just read a data page from our file. This is a good location to increase our counter!!!!!
		// ******************************
		MultiCounter.increaseCounter(1);
		
		// before returning it, cache it for future reference
		this.cacheData(dataByteOffset, result);
		
		
		return result;
		
	}
	
	public BTreeInnerNode newInnerNode() throws IOException {
		BTreeInnerNode result = new BTreeInnerNode();
		this.aquireNodeStorage(result);
		result.setDirty();
		this.cacheNode(result.getStorageDataPage(), result);
		return result;
	}

	public BTreeLeafNode newLeafNode() throws IOException {
		BTreeLeafNode result = new BTreeLeafNode();
		this.aquireNodeStorage(result);
		result.setDirty();
		this.cacheNode(result.getStorageDataPage(), result);
		return result;
	}
	
	// opens our node/index file, calculates the dataPageIndex that corresponds to the end of the file (raf.length()) 
	// and sets it on given node
	private void aquireNodeStorage( BTreeNode node) throws IOException {
		int dataPageIndex = 0;
		// open file, get length, and calculate the  dataPageIndex that corresponds to the next data page at the end of the file
		RandomAccessFile raf = new RandomAccessFile(NODE_STORAGE_FILENAME,"rw");
		if (raf.length()>0) {
			dataPageIndex = (int) (raf.length()/PageSize) ;
		}
		raf.setLength(raf.length()+PageSize);
		raf.close();		
		// Actually write DATA_PAGE_LENGTH bytes to the end file, so for that subsequent new nodes the new length is used 
		node.setStorageDataPage(dataPageIndex);
	}
	
	
	public int newData(Data result, int nextFreeDatafileByteOffset) {
		int NO_OF_DATA_BYTES = 32;
		result.setStorageByteOffset(nextFreeDatafileByteOffset);
		result.setDirty(); // so that it will written to disk at next flush
		this.cacheData(result.getStorageByteOffset(), result);
		return nextFreeDatafileByteOffset + NO_OF_DATA_BYTES;
	}
	
}
