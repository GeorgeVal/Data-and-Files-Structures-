package org.tuc.Btree;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

class BTreeLeafNode extends BTreeNode {
	protected final static int LEAFORDER = 28;
	private final static int PageSize = 256;
	// CHANGE FOR STORING ON FILE
	private Integer[] values; // integers pointing to byte offset in data file
	
	public BTreeLeafNode() {
		this.keys = new Integer[LEAFORDER + 1];
		this.values = (Integer[]) new Integer[LEAFORDER + 1];
	

	}

	public Data getData(int index) throws IOException {
		
		return StorageCache.getInstance().retrieveData(this.values[index].intValue());
	}

	public void setData(int index, Data value) {
		if(value==null) {
			this.values[index]=null;
		}
		else
		this.values[index] =  ((Data)value).getStorageByteOffset();
		setDirty(); // we changed a value, so this node is dirty and must be flushed to disk
	}
	
	@Override
	public TreeNodeType getNodeType() {
		return TreeNodeType.LeafNode;
	}
	
	@Override
	public int search(Integer key) {
		for (int i = 0; i < this.getKeyCount(); ++i) {
			 int cmp = this.getKey(i).compareTo(key);
			 if (cmp == 0) {
				 return i;
			 }
			 else if (cmp > 0) {
				 return -1;
			 }
		}
		return -1;
	}
	
	public int searchForLow(Integer key) {
		int i;
		for ( i = 0; i < this.getKeyCount(); ++i) {
			 int cmp = this.getKey(i).compareTo(key);
			 if (cmp == 0) {
				 return i;
			 }
			 else if (cmp > 0) {
				 return i;
			 }
		}
		return i;
	}
	
	
	/* The codes below are used to support insertion operation */
	
	public void insertKey(Integer key, Data value) throws IOException {
		int index = 0;
		while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0)
			++index;

		this.insertAt(index, key, value);
	}
	
	private void insertAt(int index, Integer key, Data value) throws IOException {
		// move space for the new key
		for (int i = this.getKeyCount() - 1; i >= index; --i) {
			this.setKey(i + 1, this.getKey(i));
			this.setData(i + 1, this.getData(i));
		}
		
		// insert new key and value
		this.setKey(index, key);
		this.setData(index, value);
		// setDirty() will be called in setValue/setData
		++this.keyCount;
	}
	
	
	/**
	 * When splits a leaf node, the middle key is kept on new node and be pushed to parent node.
	 * @throws IOException 
	 */
	@Override
	protected BTreeNode split() throws IOException {
		int midIndex = this.getKeyCount() / 2;
		
		BTreeLeafNode newRNode = StorageCache.getInstance().newLeafNode();
		for (int i = midIndex; i < this.getKeyCount(); ++i) {
			newRNode.setKey(i - midIndex, this.getKey(i));
			newRNode.setData(i - midIndex, this.getData(i));
			this.setKey(i, null);
			this.setData(i, null);
		}
		newRNode.keyCount = this.getKeyCount() - midIndex;
		this.keyCount = midIndex;
		setDirty();// just to make sure

		return newRNode;
	}
	
	@Override
	protected BTreeNode pushUpKey(Integer key, BTreeNode leftChild, BTreeNode rightNode) {
		throw new UnsupportedOperationException();
	}
	
	
	
	
	/* The codes below are used to support deletion operation */
	
	public boolean delete(Integer key) throws IOException {
		int index = this.search(key);
		if (index == -1)
			return false;
		
		this.deleteAt(index);
		return true;
	}
	
	private void deleteAt(int index) throws IOException {
		int i = index;
		for (i = index; i < this.getKeyCount() - 1; ++i) {
			this.setKey(i, this.getKey(i + 1));
			this.setData(i, this.getData(i + 1));
		}
		this.setKey(i, null);
		this.setData(i, null);
		--this.keyCount;
		
		// setDirty will be called through setValue/setData
	}
	
	@Override
	protected void processChildrenTransfer(BTreeNode borrower, BTreeNode lender, int borrowIndex) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	protected BTreeNode processChildrenFusion(BTreeNode leftChild, BTreeNode rightChild) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Notice that the key sunk from parent is be abandoned. 
	 * @throws IOException 
	 */
	@Override
	protected void fusionWithSibling(Integer sinkKey, BTreeNode rightSibling) throws IOException {
		BTreeLeafNode siblingLeaf = (BTreeLeafNode)rightSibling;
		
		int j = this.getKeyCount();
		for (int i = 0; i < siblingLeaf.getKeyCount(); ++i) {
			this.setKey(j + i, siblingLeaf.getKey(i));
			this.setData(j + i, siblingLeaf.getData(i));
		}
		this.keyCount += siblingLeaf.getKeyCount();
		
		this.setRightSibling((siblingLeaf.getRightSibling()));
		if (siblingLeaf.rightSibling != null)
			siblingLeaf.getRightSibling().setLeftSibling(this);
	}
	
	@Override
	protected Integer transferFromSibling(Integer sinkKey, BTreeNode sibling, int borrowIndex) throws IOException {
		BTreeLeafNode siblingNode = (BTreeLeafNode)sibling;
		
		this.insertKey(siblingNode.getKey(borrowIndex), siblingNode.getData(borrowIndex));
		siblingNode.deleteAt(borrowIndex);
		// setDirty will be called through setKey/setData in deleteAt
		return borrowIndex == 0 ? sibling.getKey(0) : this.getKey(0);
	}
	
	protected byte[] toByteArray() {
		// very similar to BTreeInnerNode. Instead of pointers to children (offset to our data pages in our node file), we have pointers
		// to data (byte offset to our data file)

		byte[] byteArray = new byte[PageSize]; // 256: demo size of our data page. This should be some constant
		int leftSibling;
		int rightSibling;
		int parent;
		int keysCount = this.getKeyCount();
		Integer[] values = this.values;
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
			out.writeInt(1);
			out.writeInt(parent);
			out.writeInt(leftSibling);
			out.writeInt(rightSibling);
			out.writeInt(keysCount);
			for(int i=0; i<keysCount; i++) {
				out.writeInt(keys[i]);
			}
			for(int i=0; i<keysCount; i++) {
				out.writeInt((int) values[i]);
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
	protected BTreeLeafNode fromByteArray(byte[] byteArray, int dataPageOffset) throws IOException {
		// this takes a byte array of fixed size, and transforms it to a BTreeLeafNode
		// it takes the format we store our node (as specified in toByteArray()) and constructs the BTreeLeafNode
		// We need as parameter the dataPageOffset in order to set it
		//BTreeLeafNode result = StorageCache.getInstance().newLeafNode();
		BTreeLeafNode result = new BTreeLeafNode();
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
			for(int i=0; i<result.keyCount; i++) {
				result.values[i] =  ois.readInt();
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
