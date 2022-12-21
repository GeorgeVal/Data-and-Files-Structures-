package org.tuc.Btree;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Contains our data. It is of fixed byte array size for writing to or reading to the data file
 * @author gv
 *
 */
public class Data {
	private int storageByteOffset; // this node is stored at byte index storageByteOffset in the data file. We must calculate the datapage this corresponds to in order to read or write it

	private int data1;
	private int data2;
	private int data3;
	private int data4;
	private int data5;
	private int data6;
	private int data7;
	private int data8;
	
	private boolean dirty;
	
	public Data() {
		this.data1 = 0;
		this.data2 = 0;
		this.data3 = 0;
		this.data4 = 0;
		this.data5 = 0;
		this.data6 = 0;
		this.data7 = 0;
		this.data8 = 0;
	}
	public Data(int data1, int data2, int data3, int data4) {
		this.data1 = data1;
		this.data2 = data2;
		this.data3 = data3;
		this.data4 = data4;
		this.data5 = data1 % 2;
		this.data6 = data2 % 2;
		this.data7 = data3 % 2;
		this.data8 = data4 % 2;
	}
	
	public boolean isDirty() {
		return this.dirty;
	}
	public void setDirty() {
		this.dirty = true;
	}
	public void setStorageByteOffset(int storageByteOffset) {
		this.storageByteOffset = storageByteOffset;
	}
	public int getStorageByteOffset() {
		return this.storageByteOffset;
	}
	
	@Override
	public String toString() {
		
		return "data1: "+data1+", data2: "+data2+", data3: "+data3+", data4: "+data4+", data5: "+data5+", data6: "+data6+", data7: "+data7+", data8: "+data8;
	}
	

	/* takes a Data class, and transforms it to an array of bytes 
	  we can't store it as is to the file. We must calculate the data page based on storageByteIndex, load the datapage, replace
	  the part starting from storageByteIndex, and then store the data page back to the file
	  */ 
	
	
	protected byte[] toByteArray() {
		// .....
		// .....
		byte[] byteArray = new byte[32]; // 32: demo size of our data. This should be some constant
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
		DataOutputStream out = new DataOutputStream(bos);
		
		  try {
				out.writeInt(data1);
				out.writeInt(data2);
				out.writeInt(data3);
				out.writeInt(data4);
				out.writeInt(data5);
				out.writeInt(data6);
				out.writeInt(data7);
				out.writeInt(data8);
				
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

	
	/* 
	 this takes a byte array of fixed size, and transforms it to a Data class instance
	 it takes the format we store our Data (as specified in toByteArray()) and constructs the Data
	 We need as parameter the storageByteIndex in order to set it
	 */
	protected Data fromByteArray(byte[] byteArray, int storageByteOffset) {
		Data result = new Data(1,2,3,4); // 1,2,3,4 will be your data extracted from the byte array
		result.setStorageByteOffset(storageByteOffset);
		
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
			DataInputStream ois = new DataInputStream(bis);
			result.data1= ois.readInt();
			result.data2= ois.readInt();
			result.data3= ois.readInt();
			result.data4= ois.readInt();
			result.data5= ois.readInt();
			result.data6= ois.readInt();
			result.data7= ois.readInt();
			result.data8= ois.readInt();
			
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
