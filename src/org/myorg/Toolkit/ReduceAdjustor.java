package org.myorg.Toolkit;

import java.util.List;
import java.util.Vector;

import java.util.Iterator;
import org.myorg.common.BatchGen_Weight;
import org.myorg.common.Common;

/**
 * @author qlxucn
 * Adjust the reducer arrangement for low-cardinality cuboids.<br>
 * Just for non-distributive operation<br>
 * <br>
 * <b>Currently only concern about '4_', '3_' and '4_3_' who are low-cardinality</b>
 */
public class ReduceAdjustor {
	private BatchGen_Weight m_bg = new BatchGen_Weight();
	private Vector<BATCHINFO> m_batches;
	private Vector<Integer> m_whole_batches;
	private Vector<Integer> m_normal_batches;
	private Vector<Integer> m_lowcard_batches;
	
	final double alpha = 1.0;
	final String m_chainSperator = Common.value_batchidSeperator;
	
	
	public ReduceAdjustor(){
		m_bg.create(Common.column_count);
		m_batches = m_bg.getBatchExt();

		if(m_batches.size()!=m_durations.length){
			System.out.println("!!!!!!!!ERROR! Duration's number is NOT correct!" + "batches_size = "+m_batches.size());
		}
		
		divideBatches();
		initBatches();
		
		//Arrange reducers to all batches
		arrangeReducers(Common.reduce_task_num, m_whole_batches);
		outputCuboids(m_batches);
		
		//Adjust reducers of low-card batches
		int remainReducerNum = adjust();
		
		//Arrange remaining reducers to normal batches
		arrangeReducers(remainReducerNum, m_normal_batches);
		
		outputCuboids(m_batches);
//		System.out.println(m_lowcard_batches);
//		System.out.println(m_normal_batches);
//		System.out.println(m_whole_batches);
	}

	/**
	 * initialize the durations
	 */
	private void initBatches() {
		Iterator<BATCHINFO> itr = m_batches.iterator();
		while(itr.hasNext()){
			BATCHINFO bi = itr.next();
			bi.duration = m_durations[bi.batchId];
		}
	}
	
	private void arrangeReducers(int totalReducerNum, Vector<Integer> vecBatches){
		int totalDuration = 0;
		Iterator<Integer> itr = vecBatches.iterator();
		while(itr.hasNext()){
			totalDuration += m_batches.get(itr.next()).duration;
		}
		
		int sum=0;
		itr = vecBatches.iterator();
		while(itr.hasNext()){
			BATCHINFO bi = m_batches.get(itr.next());
			
			int span = (int) Math.round( (double)bi.duration / (double)totalDuration * (double) totalReducerNum);
//			System.out.println(">>>"+span+"|"+bi.duration+"|"+m_totalDuration+"|"+totalReducerNum);
			bi.rNum = (span == 0) ? 1 : span;
			sum += bi.rNum;
		}
		
		m_batches.get(vecBatches.get(0)).rNum += totalReducerNum - sum;
	}

	/** Divide normal batches and low-cardinality ones
	 * 
	 */
	private void divideBatches() {
		m_lowcard_batches = new Vector<Integer>();
		m_normal_batches = new Vector<Integer>();
		m_whole_batches = new Vector<Integer>();
		
		for(int i=0;i<m_batches.size();i++){
			BATCHINFO headCuboid = m_batches.get(i);
			m_whole_batches.add(headCuboid.batchId);
			
			if(headCuboid.isLowCard()){
				m_lowcard_batches.add(headCuboid.batchId);
			}else{
				m_normal_batches.add(headCuboid.batchId);
			}
		}
		
	}

	/** Determine if they can be batched.<br><br> 
	 * <b>Currently only dispose b[10], b[11] and b[12] which are "4_3_", "3_" and, "4_"</b>
	 *  @2012.7.30
	 *  <br><br>
	 *  c10,c11,c12 <= Common.low_card<br>
	 *  c10 = c11*c12<br>
	 *  alpha = 1.0<br>
	 *  1. all can be batched: c11 >= R10+R11 ; c12 >= R10+R12;<br>
	 *  2. only c11 can be batched: c11 >= R10+R11 ; c12 < R10+R12;<br>
	 *  3. only c12 can be batched: c11 < R10+R11 ; c12 >= R10+R12;<br>
	 *  4. no one can be batched: c11 < R10+R11 ; c12 < R10+R12;<br>
	 */
	public int adjust(){
		
		BATCHINFO bi1, bi2, bi3;
		bi1 = m_batches.get(m_lowcard_batches.get(0));
		bi2 = m_batches.get(m_lowcard_batches.get(1));
		bi3 = m_batches.get(m_lowcard_batches.get(2));
		
		boolean b2 = canBatch(bi2, bi1);
		boolean b3 = canBatch(bi3, bi1);
		System.out.println(b2+":"+bi2.card+"  "+b3+":"+bi3.card);
		int totalLowcardReducer = 0;	//total reducer number for lowcard batches
		
		if(b2 && (!b3 || (b3 && bi2.size >= bi3.size))){	//batch bi2 and bi1
			bi1.chainname += m_chainSperator + bi2.chainname;	
			bi1.rNum += bi2.rNum;
			bi1.rNum = getReducerNum(bi1);
			bi3.rNum = getReducerNum(bi3);
			//remove "bi2" from batches
			m_batches.remove(bi2);
			totalLowcardReducer = bi1.rNum + bi3.rNum;
			
		}else if (b3 && (!b2 || (b2 && bi2.size < bi3.size))){	//batch bi3 and bi1
			bi1.chainname += m_chainSperator + bi3.chainname;
			bi1.rNum += bi3.rNum;
			bi1.rNum = getReducerNum(bi1);
			bi2.rNum = getReducerNum(bi2);
			//remove "bi3" from batches
			m_batches.remove(bi3);
			totalLowcardReducer = bi1.rNum + bi2.rNum;
			
		}else{ // no any batch
			bi1.rNum = getReducerNum(bi1);
			bi2.rNum = getReducerNum(bi2);
			bi3.rNum = getReducerNum(bi3);
			
			totalLowcardReducer = bi1.rNum + bi2.rNum + bi3.rNum;
		}
		
		return Common.reduce_task_num - totalLowcardReducer;
	}
	

	/**Get the exact reducer number which is enough for "bi"
	 * @param bi
	 * @return
	 */
	private int getReducerNum(BATCHINFO bi) {
		int needNum = (int)Math.round(alpha * (double)bi.card);
		int R = (bi.rNum > needNum) ? needNum: bi.rNum;
		
		return R;
	}
	
	/** determine if batch bi2 and bi1 can be batched, e.g. bi1 = "3_" and bi2 = "4_3_"
	 *
	 */
	private boolean canBatch(BATCHINFO bi1, BATCHINFO bi2) {
		return (double)bi1.card * alpha >= (double)(bi1.rNum + bi2.rNum);
	}

	private void outputCuboids(Vector<BATCHINFO> vecBI){
		System.out.println("level\t size\t batchId\t duration\t reducerNum\t chainname");
		int totalDuration = 0;
		Iterator<BATCHINFO> itr = vecBI.iterator();
		String tmp = "";
		while(itr.hasNext()){
			BATCHINFO bi = itr.next();
			tmp =  bi.level+"\t"+ bi.size+"\t"+bi.batchId +"\t"+bi.duration+"\t"+bi.rNum+ "\t"+bi.chainname;
//			tmp = bi.batchId +", ";
			System.out.println(tmp);
			
			totalDuration += bi.duration;
		}
		System.out.println("******outputCuboids*******"+totalDuration);
		
	}
	
	public static void main(String[] args) throws Exception {
		ReduceAdjustor ra = new ReduceAdjustor();
		
	}
	
	/** The data get from sampling result
	 * 
	 */
	private final int [] m_durations = {
	//13 durations from job_201207261655_0009(CubeBatched_Sample_NonDis)
			525,
			420,
			495,
			315,
			255,
			282,
			213,
			210,
			207,
			204,
			246,
			201,
			234,
	};
}
