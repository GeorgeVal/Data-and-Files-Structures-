package org.tuc.Btree;

import java.io.IOException;

/**
 * Original at https://github.com/linli2016/BPlusTree 
 * A B+ tree
 * Since the structures and behaviors between internal node and external node are different, 
 * so there are two different classes for each kind of node.
 * @param <Integer> the data type of the key
 * @param <Data> the data type of the value
 */
public class BTree {
	private BTreeNode root;
	
	private int nextFreeDatafileByteOffset = 0; // for this assignment, we only create new, empty files. We keep here the next free byteoffset in our file

	public BTree() throws IOException {
		// CHANGE FOR STORING ON FILE 
		this.root = StorageCache.getInstance().newLeafNode();
		StorageCache.getInstance().flush();
	}
	

	
	
	
	/**
	 * Insert a new key and its associated value into the B+ tree.
	 * @throws IOException 
	 */
	public void insert(Integer key, Data value) throws IOException {
				
		this.root = StorageCache.getInstance().retrieveNode(this.root.getStorageDataPage());
		

		
		// CHANGE FOR STORING ON FILE
		nextFreeDatafileByteOffset = StorageCache.getInstance().newData((Data)value, nextFreeDatafileByteOffset);

		
		// CHANGE FOR STORING ON FILE 
		BTreeLeafNode leaf = this.findLeafNodeShouldContainKey(key);

		leaf.insertKey(key, value);

		if (leaf.isOverflow()) {

			BTreeNode n = leaf.dealOverflow();

			if (n != null)
				this.root = n; 
		}
		
		// CHANGE FOR STORING ON FILE
		StorageCache.getInstance().flush();
	}
	
	/**
	 * Search a key value on the tree and return its associated value.
	 * @throws IOException 
	 */
	public Data search(Integer key) throws IOException {
		BTreeLeafNode leaf = this.findLeafNodeShouldContainKey(key);
		
		int index = leaf.search(key);
		
		return (index == -1) ? null : leaf.getData(index);
	}
	
	
	/**
	 * Searches for keys in range (lowRange highRange) 
	 * @param lowRange
	 * @param highRange
	 * @throws IOException
	 */
	public void findRange(Integer lowRange, Integer highRange) throws IOException {
		int i,index = 0;
		i = index;
		BTreeLeafNode leaf = this.findLeafNodeShouldContainKey(lowRange);
	    index = leaf.searchForLow(lowRange);
		while(leaf!=null && leaf.getKey(i)!=null) {
			for( i=index; i<leaf.getKeyCount(); i++) {
				if(leaf.getKey(i) > highRange) return;			
			}
			leaf = (BTreeLeafNode) leaf.getRightSibling(); //when all keys from leaf are checked then go to the right sibling of leaf
			index = 0;
		}
	}
	
	/**
	 * Delete a key and its associated value from the tree.
	 * @throws IOException 
	 */
	public void delete(Integer key) throws IOException {
		this.root = StorageCache.getInstance().retrieveNode(this.root.getStorageDataPage());
		BTreeLeafNode leaf = this.findLeafNodeShouldContainKey(key);
		
		if (leaf.delete(key) && leaf.isUnderflow()) {
			BTreeNode n = leaf.dealUnderflow();
			if (n != null)
				this.root = n; 
		}
		// CHANGE FOR STORING ON FILE
		StorageCache.getInstance().flush();
	}
	
	/**
	 * Search the leaf node which should contain the specified key
	 * @throws IOException 
	 */
	private BTreeLeafNode findLeafNodeShouldContainKey(Integer key) throws IOException {
		BTreeNode node = this.root;
		
		while (node.getNodeType() == TreeNodeType.InnerNode) {
			node = ((BTreeInnerNode)node).getChild( node.search(key) );
		}
		
		return (BTreeLeafNode)node;
	}
}


