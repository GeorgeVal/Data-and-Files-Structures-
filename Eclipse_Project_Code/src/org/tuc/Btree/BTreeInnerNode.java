package org.tuc.Btree;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

class BTreeInnerNode extends BTreeNode {
	protected final static int INNERORDER = 28;
	private final static int PageSize = 256;
	// CHANGE FOR STORING ON FILE
	protected Integer[] children;
	
	public BTreeInnerNode() {
		this.keys = new Integer[INNERORDER + 1];
		// CHANGE FOR STORING ON FILE
		this.children = new Integer[INNERORDER + 2];
	}
	
	public BTreeNode getChild(int index) throws IOException {
		// CHANGE FOR STORING ON FILE
		if(this.children[index]==null) {
			return null;
		}
		return (BTreeNode)StorageCache.getInstance().retrieveNode(this.children[index]);
	}

	public void setChild(int index, BTreeNode child) {
		// CHANGE FOR STORING ON FILE
		if(child == null) {
			this.children[index]=null;
		}
		else 
			this.children[index] = child.getStorageDataPage();
		
		if (child != null)
			child.setParent(this);

		
		
		setDirty();
	}
	
	@Override
	public TreeNodeType getNodeType() {
		return TreeNodeType.InnerNode;
	}
	
	@Override
	public int search(Integer key) {
		int index = 0;
		for (index = 0; index < this.getKeyCount(); ++index) {
			int cmp = this.getKey(index).compareTo(key);
			if (cmp == 0) {
				return index + 1;
			}
			else if (cmp > 0) {
				return index;
			}
		}
		
		return index;
	}
	
	
	/* The codes below are used to support insertion operation */
	
	private void insertAt(int index, Integer key, BTreeNode leftChild, BTreeNode rightChild) throws IOException {
		// move space for the new key
		for (int i = this.getKeyCount() + 1; i > index; --i) {
			this.setChild(i, this.getChild(i - 1));
		}
		for (int i = this.getKeyCount(); i > index; --i) {
			this.setKey(i, this.getKey(i - 1));
		}
		
		// insert the new key
		this.setKey(index, key);
		this.setChild(index, leftChild);
		this.setChild(index + 1, rightChild);
		this.keyCount += 1;
		
	}
	
	/**
	 * When splits a internal node, the middle key is kicked out and be pushed to parent node.
	 * @throws IOException 
	 */
	@Override
	protected BTreeNode split() throws IOException {
		int midIndex = this.getKeyCount() / 2;
		
		BTreeInnerNode newRNode = StorageCache.getInstance().newInnerNode();
		for (int i = midIndex + 1; i < this.getKeyCount(); ++i) {
			newRNode.setKey(i - midIndex - 1, this.getKey(i));
			this.setKey(i, null);
		}
		for (int i = midIndex + 1; i <= this.getKeyCount(); ++i) {
			newRNode.setChild(i - midIndex - 1, this.getChild(i));
			newRNode.getChild(i - midIndex - 1).setParent(newRNode);
			this.setChild(i, null);
		}
		this.setKey(midIndex, null);
		newRNode.keyCount = this.getKeyCount() - midIndex - 1;
		this.keyCount = midIndex;
		setDirty();
		return newRNode;
	}
	
	
	@Override
	protected BTreeNode pushUpKey(Integer key, BTreeNode leftChild, BTreeNode rightNode) throws IOException {
		// find the target position of the new key
		int index = this.search(key);
		
		// insert the new key
		this.insertAt(index, key, leftChild, rightNode);

		// check whether current node need to be split
		if (this.isOverflow()) {
			return this.dealOverflow();
		}
		else {
			return this.getParent() == null ? this : null;
		}
	}
	
	
	
	
	/* The codes below are used to support delete operation */
	
	private void deleteAt(int index) throws IOException {
		int i = 0;
		for (i = index; i < this.getKeyCount() - 1; ++i) {
			this.setKey(i, this.getKey(i + 1));
			this.setChild(i + 1, this.getChild(i + 2));
		}
		this.setKey(i, null);
		this.setChild(i + 1, null);
		--this.keyCount;
		setDirty();
	}
	
	
	@Override
	protected void processChildrenTransfer(BTreeNode borrower, BTreeNode lender, int borrowIndex) throws IOException {
		int borrowerChildIndex = 0;
		while (borrowerChildIndex < this.getKeyCount() + 1 && this.getChild(borrowerChildIndex) != borrower)
			++borrowerChildIndex;
		
		if (borrowIndex == 0) {
			// borrow a key from right sibling
			Integer upKey = borrower.transferFromSibling(this.getKey(borrowerChildIndex), lender, borrowIndex);
			this.setKey(borrowerChildIndex, upKey);
		}
		else {
			// borrow a key from left sibling
			Integer upKey = borrower.transferFromSibling(this.getKey(borrowerChildIndex - 1), lender, borrowIndex);
			this.setKey(borrowerChildIndex - 1, upKey);
		}
	}
	
	@Override
	protected BTreeNode processChildrenFusion(BTreeNode leftChild, BTreeNode rightChild) throws IOException  {
		int index = 0;
		while (index < this.getKeyCount() && this.getChild(index) != leftChild)
			++index;
		Integer sinkKey = this.getKey(index);
		
		// merge two children and the sink key into the left child node
		leftChild.fusionWithSibling(sinkKey, rightChild);
		
		// remove the sink key, keep the left child and abandon the right child
		this.deleteAt(index);
		
		// check whether need to propagate borrow or fusion to parent
		if (this.isUnderflow()) {
			if (this.getParent() == null) {
				// current node is root, only remove keys or delete the whole root node
				if (this.getKeyCount() == 0) {
					leftChild.setParent(null);
					return leftChild;
				}
				else {
					return null;
				}
			}
			
			return this.dealUnderflow();
		}
		
		return null;
	}
	
	
	@Override
	protected void fusionWithSibling(Integer sinkKey, BTreeNode rightSibling) throws IOException {
		BTreeInnerNode rightSiblingNode = (BTreeInnerNode)rightSibling;
		
		int j = this.getKeyCount();
		this.setKey(j++, sinkKey);
		
		for (int i = 0; i < rightSiblingNode.getKeyCount(); ++i) {
			this.setKey(j + i, rightSiblingNode.getKey(i));
		}
		for (int i = 0; i < rightSiblingNode.getKeyCount() + 1; ++i) {
			this.setChild(j + i, rightSiblingNode.getChild(i));
		}
		this.keyCount += 1 + rightSiblingNode.getKeyCount();
		
		this.setRightSibling(rightSiblingNode.getRightSibling());
		if (rightSiblingNode.rightSibling != null)
			rightSiblingNode.getRightSibling().setLeftSibling(this);
	}
	
	@Override
	protected Integer transferFromSibling(Integer sinkKey, BTreeNode sibling, int borrowIndex) throws IOException {
		BTreeInnerNode siblingNode = (BTreeInnerNode)sibling;
		
		Integer upKey = null;
		if (borrowIndex == 0) {
			// borrow the first key from right sibling, append it to tail
			int index = this.getKeyCount();
			this.setKey(index, sinkKey);
			this.setChild(index + 1, siblingNode.getChild(borrowIndex));			
			this.keyCount += 1;
			
			upKey = siblingNode.getKey(0);
			siblingNode.deleteAt(borrowIndex);
		}
		else {
			// borrow the last key from left sibling, insert it to head
			this.insertAt(0, sinkKey, siblingNode.getChild(borrowIndex + 1), this.getChild(0));
			upKey = siblingNode.getKey(borrowIndex);
			siblingNode.deleteAt(borrowIndex);
		}
		setDirty();
		return upKey;
	}
	
	protected byte[] toByteArray()  {
		
		// must include the index of the data page to the left sibling (int == 4 bytes), to the right sibling,
		// to the parent node, the number of keys (keyCount), the type of node (inner node/leaf node) and the list of keys and list of children (each key 4 byte int, each children 4 byte int pointing to the a data page offeset)
		// We do not need the isDirty flag and the storageDataPage
		// so we need
		// 4 bytes for marking this as a inner node (e.g. an int with value = 1 for inner node and 2 for leaf node)
		// 4 bytes for left sibling
		// 4 bytes for right sibling
		// 4 bytes for parent
		// 4 bytes for the number of keys
		// The rest in our data page are for the list of pointers to children, and the keys. Depending on the size of our data page
		// we can calculate the order our tree
		byte[] byteArray = new byte[PageSize]; // 256: demo size of our data page. This should be some constant
		int leftSibling;
		int rightSibling;
		int parent;
		int keysCount = this.getKeyCount();
		Integer[] children = this.children;
		Integer[] keys = this.keys;
		if (this.leftSibling != null) {
			 leftSibling = this.leftSibling;
		}
		else  leftSibling = -1;
		if (this.rightSibling != null) {
			 rightSibling = this.rightSibling;
		}
		else  rightSibling = -1;
		if (this.parentNode != null) {
			 parent = this.parentNode;
		}
		else  parent = -1;
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
		DataOutputStream out = new DataOutputStream(bos);
        	
        try {
			out.writeInt(0);
			out.writeInt(parent);
			out.writeInt(leftSibling);
			out.writeInt(rightSibling);
			out.writeInt(keysCount);
			for(int i=0; i<keysCount; i++) {
				out.writeInt(keys[i]);
			}
			for(int i=0; i<keysCount+1; i++) {
				out.writeInt(children[i]);
			}
			
			int bosSize=bos.size();
			if((256-bosSize)>0) {
				for(int i =0; i<256-bosSize; i++) {
					out.writeByte(0);
				}
			}
			
			out.close();
			byteArray = bos.toByteArray();
		    bos.close();
			
		}
		
		 catch (IOException e) {
			System.out.println("toByteArray failed");
			e.printStackTrace();		
		}

		return byteArray;
		
	}
	protected BTreeInnerNode fromByteArray(byte[] byteArray, int dataPageOffset) throws IOException {
		// this takes a byte array of fixed size, and transforms it to a BTreeInnerNode
		// it takes the format we store our node (as specified in BTreeInnerNode.toByteArray()) and constructs the BTreeInnerNode
		// We need as parameter the dataPageOffset in order to set it
		
		BTreeInnerNode result = new BTreeInnerNode();
		result.setStorageDataPage(dataPageOffset);
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
			DataInputStream ois = new DataInputStream(bis);
			ois.readInt();
			result.parentNode = ois.readInt();
			if(result.parentNode == -1) {
				result.parentNode=null;
			}
			result.leftSibling = ois.readInt();
			if(result.leftSibling == -1) {
				result.leftSibling=null;
			}
			result.rightSibling = ois.readInt();
			if(result.rightSibling == -1) {
				result.rightSibling=null;
			}
			result.keyCount = ois.readInt();
			for(int i=0; i<result.keyCount; i++) {
				result.keys[i]= ois.readInt();
			}
			
			for(int i=0; i<result.keyCount+1; i++) {
				result.children[i] = ois.readInt();
			}
			ois.close();
			bis.close();
		
			
		}
		catch (IOException e) {
			System.out.println("fromByteArray failed");
			e.printStackTrace();		
		}
		return result;
		
	}
}