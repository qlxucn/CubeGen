/*
 * cuboid-name's format is like "c1_c2_c3", divided by '_'
 * 
 * */

package org.myorg.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.mapred.JobConf;

public class Global { 

	public static class RANGE {
		public int start = 0;
		public int span = 0;
	}

	// name | batchId : cubeInfoByCuboId
	public static class NAME_BATCHID {
		public String name;
		public int batchId;
	}

	// tag | <cuboId> | range | weight : cubeInfoByBatchId
	public static class TAG_CUBOIDS_RANGE_WEIGHT {
		public BitSet tag = new BitSet();
		public Vector<Integer> cuboIds = new Vector<Integer>();
		public RANGE range = new RANGE();
		public double weight;
	}

	public int nSplitNum = -1;
	// public final String separator = "|";
	public int numReduceTask = -1;
	public int batchCount = -1;
	public int cuboidCount = -1;
	public int columnCount = -1;
	private BatchGen_Weight bg = new BatchGen_Weight();

	public void initGlobal(JobConf jobConf) {
		columnCount = jobConf.getInt("Column.Count", -1);
		nSplitNum = columnCount + 1;

		bg.create(columnCount);
		batchCount = bg.getBatchCount();

		cuboidCount = (int) Math.pow(2, columnCount);
		numReduceTask = jobConf.getNumReduceTasks();
		// isDistributive = jobConf.getNumReduceTasks();

		/**
		 * The order of initialization is
		 * "initCubeInfoByCuboId->initCubeInfoByBatchId->initCubeInfoByTag"
		 * */
		initCubeInfoByCuboId(bg.getBatch());

		String operator = jobConf.getJobOperator();
		boolean isDistributive = operator.equals("sum");
		
		try {
			// weights were generated outside, its index is "batchId"
			// If the batch is low-cardinality, then its weight should be 0.  
			Double[] weights = getWeights(isDistributive);
			List<Integer> spanList = getRangeSpanList(numReduceTask, weights);
			initCubeInfoByBatchId(spanList);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		// initCubeInfoByTag();
	}

	// count the the number of '1'
	public int getCubeNum(BitSet tag) {
		// System.out.println("getCubeNum tag[0] = "+String.valueOf(tag[0]));
		return tag.cardinality();
	}

	// <cubeoId, name|batchId>
	public HashMap<Integer, NAME_BATCHID> cubeInfoByCuboId = new HashMap<Integer, NAME_BATCHID>();

	private void putCubeInfo(int id, String name, int batchId) {
		NAME_BATCHID value = new NAME_BATCHID();
		value.name = name;
		value.batchId = batchId;
		cubeInfoByCuboId.put(id, value);
	}

	public void initCubeInfoByCuboId(Vector<Vector<String>> batchSet) {
		int index = 0;
		for (int batchId = 0; batchId < batchSet.size(); batchId++) {
			for (int j = 0; j < batchSet.get(batchId).size(); j++) {
				putCubeInfo(index, batchSet.get(batchId).get(j), batchId);
				index++;
			}
		}
	}

	// Get name by tag
	public String getCuboidName(BitSet tag, int no) {
		// int tmp = no;
		int index = 0;
		while (index != -1) {
			index = tag.nextSetBit(index);
			if (index != -1) {
				no--;
				if (no == 0) {
					return this.cubeInfoByCuboId.get(index).name;
				}
				index++;
			}
		}
		// System.out.println(String.valueOf(tag) + "no="+String.valueOf(tmp));
		return "CuboidNameError";
	}

	// // <tag, batchId>
	// public HashMap<BitSet, Integer> cubeInfoByTag = new HashMap<BitSet,
	// Integer>();
	//
	// public void initCubeInfoByTag() {
	// Iterator<Integer> itr = cubeInfoByBatchId.keySet().iterator();
	// while (itr.hasNext()) {
	// Integer batchId = itr.next();
	// BitSet tag = (BitSet) cubeInfoByBatchId.get(batchId).tag.clone();
	// cubeInfoByTag.put(tag, batchId);
	// }
	// }

	// <batchId, tag|<cuboId>|range|weight>
	public HashMap<Integer, TAG_CUBOIDS_RANGE_WEIGHT> cubeInfoByBatchId = new HashMap<Integer, TAG_CUBOIDS_RANGE_WEIGHT>();

	public void initCubeInfoByBatchId(List<Integer> spanList) {
		// for (int batchId = 0; batchId < this.batchCount; batchId++) {
		// from the small batch to large one
		for (int batchId = this.batchCount - 1; batchId >= 0; batchId--) {
			TAG_CUBOIDS_RANGE_WEIGHT tcrw = new TAG_CUBOIDS_RANGE_WEIGHT();

			// Generate the "<cuboId>|tag" by specific batchId
			Iterator<Integer> itr = cubeInfoByCuboId.keySet().iterator();
			while (itr.hasNext()) {
				Integer cuboId = itr.next();
				NAME_BATCHID name_batchid = cubeInfoByCuboId.get(cuboId);
				if (name_batchid.batchId == batchId) {
					tcrw.cuboIds.add(cuboId);
					tcrw.tag.set(cuboId);
				}
			}

			// Generate "range" by batchId and weight
			TAG_CUBOIDS_RANGE_WEIGHT lastTcrw = cubeInfoByBatchId
					.get(batchId + 1);
			if (null != lastTcrw) {
				tcrw.range.start = lastTcrw.range.start + lastTcrw.range.span;
			} else {
				tcrw.range.start = 0;
			}

			// If get span list, then use it; else use default
			if (spanList != null && spanList.size() == this.batchCount) {
				tcrw.range.span = spanList.get(batchId);
			} else {
//				System.out
//						.println("XXL: Global.initCubeInfoByBatchId():Not use the spanList.");
				// Generate "weight" by cuboIds's number for specific batchId
				int cuboidNum = tcrw.cuboIds.size();
				double _weight = (double) cuboidNum
						/ (double) (this.cuboidCount - 1);
				tcrw.weight = _weight;
				int tmp = (int) (tcrw.weight * this.numReduceTask);
				tcrw.range.span = (tmp == 0) ? 1 : tmp;
			}

			// System.out.println("initCubeInfoByBatchId(): batchId = "
			// + String.valueOf(batchId) + " tag= "+tcrw.tag+" range.start = "
			// + String.valueOf(tcrw.range.start) + " range.span = "
			// + String.valueOf(tcrw.range.span) + " numReduceTask = "
			// + String.valueOf(numReduceTask) + " tcrw.weight = "
			// + String.valueOf(tcrw.weight)+"  cuboidCount="+this.cuboidCount);

			// Add the <key, value> pair to hashmap
			cubeInfoByBatchId.put(batchId, tcrw);
		}

		// adjust the largest batch's span; the largest one is the first batch
		// in the 'cubeInfoByBatchId'
		TAG_CUBOIDS_RANGE_WEIGHT firstTcrw = cubeInfoByBatchId.get(0);
		firstTcrw.range.span = this.numReduceTask - firstTcrw.range.start;
		// System.out.println("adjust...:initCubeInfoByBatchId(): batchId = "
		// + String.valueOf(0) + " tag= "+firstTcrw.tag+" range.start = "
		// + String.valueOf(firstTcrw.range.start) + " range.span = "
		// + String.valueOf(firstTcrw.range.span) + " numReduceTask = "
		// + String.valueOf(numReduceTask) + " tcrw.weight = "
		// +
		// String.valueOf(firstTcrw.weight)+"  cuboidCount="+this.cuboidCount);

	}

	public void printBatchInfo() {

		for (int batchId = 0; batchId < this.batchCount; batchId++) {
			TAG_CUBOIDS_RANGE_WEIGHT tcrw = cubeInfoByBatchId.get(batchId);
			
			String out = "";
			Iterator<Integer> itr = tcrw.cuboIds.iterator();
			while(itr.hasNext()){
				out += ", " + cubeInfoByCuboId.get(itr.next()).name;
			}

			out = "printBatchInfo(): batchId = "
					+ String.valueOf(batchId) + " tag= " + tcrw.tag 
					+ " name = " + out 
					+ " range.start = " + String.valueOf(tcrw.range.start)
					+ " range.span = " + String.valueOf(tcrw.range.span)
					+ " numReduceTask = " + String.valueOf(numReduceTask)
					+ "  cuboidCount=" + this.cuboidCount;
			
			
			System.out.println(out);
			
		}
	}
	
	/**Get weights from external file
	 * @param isDistributive
	 * @return	weights vector
	 * @throws IOException
	 */
	private Double[] getWeights(boolean isDistributive) throws IOException{
		File file = new File(Common.weightListPath);
//		System.out.println(Common.weightListPath);
		if(!file.exists() || file.isDirectory()){
			throw new FileNotFoundException();
		}
		
		BufferedReader br=new BufferedReader(new FileReader(file));
		String type = isDistributive? "#dis": "#nondis";
		
		//Find the area of "#dis" or "nondis"
		while(!br.readLine().equals(type)){
			//do-nothing
		}	
		
		String tmp=null;
		Double total = new Double(0);;
		Vector<Double> vec = new Vector<Double>();
		
		//get all times
		while((tmp=br.readLine()) != null && !tmp.equals("###")){
			Double a = Double.valueOf(tmp);
			vec.add(a);
			total += a;
		}
		
		//change time to weights
		for(int i=0;i<vec.size();i++){
			vec.set(i, vec.get(i)/total);
		}
		
//		System.out.println("weights = "+vec); //
	
		Double[] arr = new Double[vec.size()];
		return vec.toArray(arr);
	}

	/**
	 * Get the spanList from the sample data
	 * 
	 * @return span list
	 * @throws IOException 
	 */
	public List<Integer> getRangeSpanList(int reduceTaskNum, Double[] weights){
		List<Integer> spanList = new ArrayList<Integer>(weights.length);
		
		// Handle the low-cardinality batches' span
		int nLowCard = 0;
		for(int i=0;i<weights.length;i++){
			if(0.0 == weights[i]){
				nLowCard++;
			}
			spanList.add(1);
		}
		reduceTaskNum -= nLowCard;
		
//		System.out.println("@@@@@spanList="+spanList);
		
		// Handle the normal batches' span
		int sum = 0;
		for (int i = 0; i < weights.length; i++) {
			if(0.0 != weights[i]){
				int span = (int) Math.round(weights[i] * (double) reduceTaskNum);
				span = (span == 0) ? 1 : span;
				spanList.set(i, span);
				sum += span;
			}	
		}

//		System.out.println("@@@@@spanList="+spanList);
		// assign the remaining reducers to the longest batch
		int firstValueRevised = reduceTaskNum
				- (sum - spanList.get(0));
		spanList.set(0, firstValueRevised);

//		System.out.println("@@@@@spanList="+spanList);
		return spanList;
	}

	/**
	 * Get value form string 'value_batchid'; eg: "123,456" -> 123
	 * 
	 * @param value_batchid
	 * @return value
	 */
	public int getFirstHalf(String value_batchid) {
		int i = value_batchid.indexOf(Common.value_batchidSeperator);
		return Integer.valueOf(value_batchid.substring(0, i).trim());
	}

	/**
	 * Get batchId form string 'value_batchid'; eg: "123,456" -> 456
	 * 
	 * @param value_batchid
	 * @return batchId
	 */
	public int getLastHalf(String value_batchid) {
		int i = value_batchid.indexOf(Common.value_batchidSeperator);
		return Integer.valueOf(value_batchid.substring(i + 1).trim());
	}
	
	
	/** Set job's configuration consistently
	 * @param conf
	 * @return conf
	 */
	public static JobConf setJobEnv(JobConf conf){
		conf.set("mapred.child.java.opts","-Xmx1600m");
		conf.set("io.sort.mb", "600");		
		conf.set("io.sort.factor", "60");	
//		conf.setNumMapTasks(200);
		//conf.setBoolean("mapred.compress.map.output", true);
		
		return conf;
	}
}

/*weights for 4d dis*/
//final double[] weights_dis = {
//		0.2545945945945946,
//		0.1881081081081081,
//		0.17513513513513512,
//		0.14540540540540542,
//		0.12,
//		0.11675675675675676};

/*weights for 3d dis*/
//final double[] weights_dis = {
//		0.363636363636364,
//		0.318181818181818,
//		0.318181818181818
//		};

/*weights for 5d dis*/
//final double[] weights_dis = {
//		0.133938706015891,
//		0.118047673098751,
//		0.12372304199773,
//		0.113507377979569,
//		0.112372304199773,
//		0.081725312145289,
//		0.080590238365494,
//		0.087400681044268,
//		0.085130533484676,
//		0.063564131668558
//		};




