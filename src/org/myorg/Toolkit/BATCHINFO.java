package org.myorg.Toolkit;

import org.myorg.common.Common;

public class BATCHINFO {
	int batchId;
	String name;
	String chainname;
	int level;
	int card;
	/**
	 * reducer number
	 */
	int rNum = -1;	
	
	/**
	 * time consuming
	 */
	int duration = -1; 
	int size;
	
	public BATCHINFO(int batchId, String name, String chainname, int level, int size, int card) {
		this.batchId = batchId;
		this.name = name;
		this.chainname = chainname;
		this.level = level;
		this.size = size;
		this.card = card;
	}
	
	public boolean isLowCard(){
		return card <= Common.low_card;
	}
}
