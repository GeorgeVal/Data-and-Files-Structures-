package org.tuc.Btree;

import java.io.IOException;

enum TreeNodeType {
	InnerNode,
	LeafNode
}

abstract class BTreeNode {
	protected Integer[] keys;
	protected int keyCount;
	// CHANGE FOR STORING ON FILE
	protected Integer parentNode;
	protected Integer leftSibling;
	protected Integer rightSibling;	
	
	private boolean dirty;
	private int storageDataPage; // this node is stored at data page storageDataPage in the node/index file
	

	protected BTreeNode() {
		this.keyCount = 0;
		this.parentNode = null;
		this.leftSibling = null;
		this.rightSibling = null;
		
		this.dirty = false;
		this.storageDataPage = -1;
	}

	public void setStorageDataPage(int storageDataPage) {
		this.storageDataPage = storageDataPage;
	}
	public int getStorageDataPage() {
		return this.storageDataPage;
	}
	
	public boolean isDirty() {
		return this.dirty;
	}
	public void setDirty() {
		this.dirty = true;
	}
	
	public int getKeyCount() {
		return this.keyCount;
	}
	
	public Integer getKey(int index) {
		return (Integer)this.keys[index];
	}

	public void setKey(int index, Integer key) {
		setDirty(); // we changed a key, so this node is dirty and must be flushed to disk
		this.keys[index] =  key;
	}

	public BTreeNode getParent() throws IOException {
		// CHANGE FOR STORING ON FILE
		if(this.parentNode != null ) {
			return StorageCache.getInstance().retrieveNode(this.parentNode);
		}
		return null;
	}

	public void setParent(BTreeNode parent) {
				// CHANGE FOR STORING ON FILE
				if(parent == null) {
					this.parentNode = null;
				}
				else {
					this.parentNode = parent.getStorageDataPage();
				}
				setDirty(); // we changed a sibling, so this node is dirty and must be flushed to disk
		
		
		 // we changed the parent, so this node is dirty and must be flushed to disk
		//this.parentNode = parent;
		
		// CHANGE FOR STORING ON FILE
		
	}	
	
	public abstract TreeNodeType getNodeType();
	
	
	/**
	 * Search a key on current node, if found the key then return its position,
	 * otherwise return -1 for a leaf node, 
	 * return the child node index which should contain the key for a internal node.
	 */
	public abstract int search(Integer key);
	
	
	
	/* The codes below are used to support insertion operation */
	
	public boolean isOverflow() {
		return this.getKeyCount() == this.keys.length;
	}
	
	public BTreeNode dealOverflow() throws IOException {
		
		int midIndex = this.getKeyCount() / 2;
		Integer upKey = this.getKey(midIndex);
		
		BTreeNode newRNode = this.split();
				
		if (this.getParent() == null) {
			BTreeInnerNode newParent = StorageCache.getInstance().newInnerNode();
			this.setParent(newParent);
		}
		newRNode.setParent(this.getParent());
		
		// maintain links of sibling nodes
		newRNode.setLeftSibling(this);
		newRNode.setRightSibling(this.getRightSibling());
		if (this.getRightSibling() != null)
			this.getRightSibling().setLeftSibling(newRNode);
		this.setRightSibling(newRNode);
		
		// push up a key to parent internal node
		return this.getParent().pushUpKey(upKey, this, newRNode);
	}
	
	

	
	protected abstract BTreeNode split() throws IOException;
	
	protected abstract BTreeNode pushUpKey(Integer key, BTreeNode leftChild, BTreeNode rightNode) throws IOException;
	
	
	
	
	
	
	/* The codes below are used to support deletion operation */
	
	public boolean isUnderflow() {
		return this.getKeyCount() < (this.keys.length / 2);
	}
	
	public boolean canLendAKey() {
		return this.getKeyCount() > (this.keys.length / 2);
	}
	
	public BTreeNode getLeftSibling() throws IOException {
		if (this.leftSibling != null && StorageCache.getInstance().retrieveNode(this.leftSibling).getParent() == this.getParent())
			return StorageCache.getInstance().retrieveNode(this.leftSibling);
		return null;
	}

	public void setLeftSibling(BTreeNode sibling) {
				// CHANGE FOR STORING ON FILE
				if(sibling == null) {
					this.leftSibling = null;
				}
				else {
					this.leftSibling = sibling.getStorageDataPage();
				}
				setDirty(); // we changed a sibling, so this node is dirty and must be flushed to disk
	}

	public BTreeNode getRightSibling() throws IOException {
		
		
		if (this.rightSibling != null && StorageCache.getInstance().retrieveNode(this.rightSibling).getParent() == this.getParent()) {
			
			return StorageCache.getInstance().retrieveNode(this.rightSibling);
		}
		return null;
	}

	public void setRightSibling(BTreeNode sibling) {
		// CHANGE FOR STORING ON FILE
		if(sibling == null) {
			this.rightSibling = null;
		}
		else {
			this.rightSibling = sibling.getStorageDataPage();
		}
		setDirty(); // we changed a sibling, so this node is dirty and must be flushed to disk

	}
	
	public BTreeNode dealUnderflow() throws IOException {
		if (this.getParent() == null)
			return null;
		
		// try to borrow a key from sibling
		BTreeNode leftSibling = this.getLeftSibling();
		if (leftSibling != null && leftSibling.canLendAKey()) {
			this.getParent().processChildrenTransfer(this, leftSibling, leftSibling.getKeyCount() - 1);
			return null;
		}
		
		BTreeNode rightSibling = this.getRightSibling();
		if (rightSibling != null && rightSibling.canLendAKey()) {
			this.getParent().processChildrenTransfer(this, rightSibling, 0);
			return null;
		}
		
		// Can not borrow a key from any sibling, then do fusion with sibling
		if (leftSibling != null) {
			return this.getParent().processChildrenFusion(leftSibling, this);
		}
		else {
			return this.getParent().processChildrenFusion(this, rightSibling);
		}
	}
	
	protected abstract void processChildrenTransfer(BTreeNode borrower, BTreeNode lender, int borrowIndex) throws IOException;
	
	protected abstract BTreeNode processChildrenFusion(BTreeNode leftChild, BTreeNode rightChild) throws IOException;
	
	protected abstract void fusionWithSibling(Integer sinkKey, BTreeNode rightSibling) throws IOException;
	
	protected abstract Integer transferFromSibling(Integer sinkKey, BTreeNode sibling, int borrowIndex) throws IOException;
	
	/* transforms this node to array of bytes, of length data page length */
	protected abstract byte[] toByteArray();
	
	/* converts given array bytes of fixed length of our data page to a Node */
	protected abstract BTreeNode fromByteArray(byte[] byteArray, int dataPageOffset) throws IOException;

}