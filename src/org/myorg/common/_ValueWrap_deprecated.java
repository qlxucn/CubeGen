package org.myorg.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class _ValueWrap_deprecated implements Writable {

	private int attribute = 0;
	private BitSet batchTag;

	private BitSet tmpBits;
	private byte[] tmpBytes;

	//Use for readFields()
	public _ValueWrap_deprecated() {	
		this.tmpBits = new BitSet();
		//System.out.println("####ValueWrap()");
	}
	
	//Use for write()
	public _ValueWrap_deprecated(int cuboidCount) {
		this.tmpBytes = new byte[cuboidCount / 8 + 1];
		
		//System.out.println("^^^^^^^^ValueWrap(int cuboidCount)");
	}
	
	public void setAttribute(int attr) {
		this.attribute = attr;
	}

	public void setBatchTag(BitSet tag) {
		this.batchTag = tag;
	}

	public int getAttribute() {
		return this.attribute;
	}

	public BitSet getBatchTag() {
		return this.batchTag;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// Use 'tmpBits'

		this.attribute = WritableUtils.readVInt(in);
		byte[] b = WritableUtils.readCompressedByteArray(in);
		this.batchTag = fromByteArray(b);
		// System.out.println("ValueWrap.readFields: batchtag = "+String.valueOf(this.batchTag[0]));

	}

	@Override
	public void write(DataOutput out) throws IOException {
		// Use 'tmpBytes[]'
		byte b[] = toByteArray(this.batchTag);

		WritableUtils.writeVInt(out, this.attribute);
		WritableUtils.writeCompressedByteArray(out, b);
		// System.out.println("ValueWrap.write: batchtag = "+String.valueOf(this.batchTag[0]));
	}

	// Returns a bitset containing the values in bytes.
	// The byte-ordering of bytes must be big-endian which means the most
	// significant bit is in element 0.
	private BitSet fromByteArray(byte[] bytes) {
		BitSet bits = getTmpBits();
		for (int i = 0; i < bytes.length * 8; i++) {
			if ((bytes[bytes.length - i / 8 - 1] & (1 << (i % 8))) != 0) {
				bits.set(i);
			}
		}
		//System.out.println("fromByteArray: "+String.valueOf(this.tmpBits));
		return bits;
	}

	// Returns a byte array of at least length 1.
	// The most significant bit in the result is guaranteed not to be a 1
	// (since BitSet does not support sign extension).
	// The byte-ordering of the result is big-endian which means the most
	// significant bit is in element 0.
	// The bit at index 0 of the bit set is assumed to be the least significant
	// bit.
	private byte[] toByteArray(BitSet bits) {
		byte[] bytes = getTmpBytes();
		for (int i = 0; i < bits.length(); i++) {
			if (bits.get(i)) {
				bytes[bytes.length - i / 8 - 1] |= 1 << (i % 8);
			}
		}
		return bytes;
	}

	//clear the tmpBytes
	private byte[] getTmpBytes() {
		for(int i=0;i<this.tmpBytes.length;i++)
		{
			this.tmpBytes[i] = 0;
		}
		return this.tmpBytes;
	}

	//clear the tmpBits
	private BitSet getTmpBits() {
		this.tmpBits.clear();
		return this.tmpBits;
	}

}