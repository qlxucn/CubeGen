package org.myorg.common;

import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

import org.myorg.Toolkit.BATCHINFO;

public class BatchGen_Weight {
	private class VERTEX {
		public int index = 0;
		public String headname = "";
		public String chainname = "";
		public int weight = 0;
		public int level = 0;
		public boolean used = false;
		public int card;
		
		public VERTEX(int _index, String _headname, String _chainname, int _level, int _weight, boolean _used, int _card){
			index = _index;
			headname = _headname;
			chainname = _chainname;
			level = _level;
			used = _used;
			weight = _weight;
			card = _card;
		}
		
		public VERTEX(){
			
		}
		
		public boolean isLowCard(){
			return card <= Common.low_card;
		}
		
		public int compareTo(VERTEX u){
			if(this.weight == u.weight){
				return 0;
			}
			else{
				return ((this.weight > u.weight) ? 1: -1);
			}
		}
		
		public boolean nameEquals(String name){
			String[] L1 = this.headname.split(m_nameSperator);
			String[] L2 = name.split(m_nameSperator);
			
			if(L1.length != L2.length){
				return false;
			}
			
			for (int i = 0; i < L2.length; i++) {
				boolean bExist = false;
				for (int j = 0; j < L1.length; j++) {
					if (L1[j].equals(L2[i])) {
						bExist = true;
					}
				}
				if (!bExist) {
					return false;
				}
			}

			return true;	
		}
		
		/**determine if 'u' is a part of this<br>
		 * e.g. <br>if this.headname is "abc" and u.headname is "ca", then return true
		 * @param u
		 * @return
		 */
		public boolean contains(VERTEX u){
			if(u.headname.equals("*")){
				return true;
			}
			
			String[] L = this.headname.split(m_nameSperator);
			String[] subL = u.headname.split(m_nameSperator);
			for (int i = 0; i < subL.length; i++) {
				boolean bExist = false;
				for (int j = 0; j < L.length; j++) {
					if (L[j].equals(subL[i])) {
						bExist = true;
					}
				}
				if (!bExist) {
					return false;
				}
			}

			return true;	
		}

		/**According to 'name'，adjust 'nameExt' and return the new 'nameExt'
		 * @param name
		 * @param nameExt
		 * @return
		 * e.g. name＝"4_5_", nameExt＝"2_4_3_5_6_", then 'nameExt' is "4_5_2_3_6_" after adjusted.
		 */
		private String adjustNameExt(String name, String nameExt) {
			if(name.equals("*")){
				return nameExt;
			}
			
			String result = name;
			String[] s1 = name.split(m_nameSperator);
			String[] s2 = nameExt.split(m_nameSperator);
			
			for(int i=0;i<s2.length;i++){
				boolean bExist = false;
				for(int j=0;j<s1.length;j++){
					if(s2[i].equals(s1[j])){
						bExist = true;
						break;
					}
				}
				
				if(!bExist){
					result += m_nameSperator + s2[i];
				}
			}

			StringTokenizer itr = new StringTokenizer(result, m_nameSperator);
			result = "";
			while (itr.hasMoreTokens()) {
				result += itr.nextToken() + m_nameSperator;
			}

			return result;
		}
		
		public void adjustChainName(){
			String[] names = this.chainname.split(m_chainSperator);

			for(int i=names.length-1; i>0; i--){
				names[i-1] = adjustNameExt(names[i], names[i-1]);
			}
			
			String tmp = "";
			for(int i=0;i<names.length;i++){
				//filter cuboid '*' from chain
				if(!names[i].equals("*")){
					tmp += names[i] + m_chainSperator;
				}
				
			}
			
			if(tmp.equals("")){
				tmp = "*";
			}
			
			this.chainname = tmp;
		}
	}
	
	private int m_eleNum;// Number of elements
	private Vector<Integer>[] m_levels;// 数组脚标为元素个数
	private VERTEX [] m_cuboids;
	private Vector<Integer> m_weights;
	private Integer [] m_cards;	//cardinality of each element
	/**
	 * key is the index of vertex in level(i), value is the index set of vertexes in level(i+1)
	 */
	private HashMap<Integer, Vector<Integer> > m_edges = new HashMap<Integer, Vector<Integer> >();
	private Vector<Vector<String>> m_batchSet = new Vector<Vector<String>>();// 不包括空集的batch
	public final String m_nameSperator = Common.nameSeperator;
	public final String m_chainSperator = Common.value_batchidSeperator;
//	private final String m_nameSperator = "_";
//	public final String m_chainSperator = ",";
	
	/**Create the batch set
	 * @param weights is the weight list of the attributes
	 * <br>wg's index is the attribute name.
	 */
	private void create(Vector<Integer> weights) {
		m_eleNum = weights.size();
		m_weights = (Vector<Integer>)weights.clone();
		m_levels = new Vector[m_eleNum + 1];
		m_cards = new Integer[m_eleNum];
		
		int count = (int) Math.pow(2, m_eleNum);
		m_cuboids = new VERTEX[count];		// m_cuboids[0] is '*'
		
		for (int i = 0; i < m_eleNum + 1; i++) {
			m_levels[i] = new Vector<Integer>();
		}
		
		//Initialize cardinality for each element
		initCards();
		
		// Generate the V, E, L
		genCuboids();
		genEdges();
		
		// Combine the cuboids to batch
		combine();
			
		// Generate batches
		genBatches();
	}
	
	/** Initialize the cardinality for each element <br>
	 * <br>
	 *  0_     rflag             "rflag" //62	<br>
	 *	1_     quantity          //89999		<br>
	 *	2_     sdate             //>100			<br>
	 *	3_     shipinstruct     "instruct"//2	<br>
	 *	4_     shipmode      	"smode"    //2	<br>
	 */
	private void initCards(){
		m_cards[0] = 62;
		m_cards[1] = 1000;
		m_cards[2] = 200;
		m_cards[3] = 2;
		m_cards[4] = 2;
//		m_cards[0] = 150;
//		m_cards[1] = 200;
//		m_cards[2] = 200;
//		m_cards[3] = 40;
//		m_cards[4] = 42;
	}
	
	public void create(int xxx) {
		Vector<Integer> weights = new Vector<Integer>();
		weights.add(1);	//0_
		weights.add(5);	//1_
		weights.add(10);//2_
		weights.add(20);//3_
		weights.add(40);//4_

//		weights.add(40);
//		weights.add(20);
//		weights.add(10);
//		weights.add(5);
//		weights.add(1);
		create(weights);
	}
	
	public Vector<Vector<String>> getBatch() {
		return m_batchSet;
	}

	public int getBatchCount() {
		return m_batchSet.size();
	}
	
	/**Generate L and V
	 * 
	 */
	private void genCuboids() {
		int flag = 0;
		int flagTmp = flag;
		int level;
		int weight;
		int card;
		String name = new String();

		for (int i = 0; i < m_cuboids.length; i++) {
			name = "";
			level = 0;
			weight = 0;
			card = 1;
			flagTmp = flag;
			for (int j = m_eleNum; j > 0; j--) {
				if ((flagTmp & 0x01) > 0) {
					name += String.valueOf(j - 1) + m_nameSperator;
					card *= m_cards[j-1];
					weight += m_weights.get(j-1);
					level++;
				}
				flagTmp = flagTmp >>> 1;
			}
			
			name = (name.equals("") ? "*" : name);
			VERTEX v = new VERTEX(i, name, name, level, weight, false, card);
			m_cuboids[i] = v;
			addVertex(m_levels[level], i);
//			System.out.println(i + ": "+name+", "+level+", "+weight);
			flag++;
		}
		
		//outputLevels();
	}
	
	
	/**insert 'i' into 'vector' and keep the new set ordered by desc
	 * @param vec is an ordered set by desc
	 * @param i is the index of cuboid in 'm_cuboids'
	 */
	private void addVertex(Vector<Integer> vec, Integer i) {
		if(vec.isEmpty()){
			vec.add(i);
			return;
		}

		VERTEX v1 = m_cuboids[i];
		int pos=0;
		while(pos < vec.size()){
			VERTEX v2 = m_cuboids[vec.get(pos)];
			if(1 == v1.compareTo(v2)){
				vec.insertElementAt(i, pos);
				return;
			}
			pos++;
		}
		
		vec.insertElementAt(i, pos);
	}
	
	
	/**Generate 'E'
	 * 
	 */
	private void genEdges() {
		for(int l=0; l < m_levels.length-1; l++){
			Vector<Integer> vec1 = m_levels[l];
			Vector<Integer> vec2 = m_levels[l+1];
			for(int i=0;i<vec1.size();i++){
				VERTEX v1 = m_cuboids[vec1.get(i)];
				for(int j=0;j<vec2.size();j++){
					VERTEX v2 = m_cuboids[vec2.get(j)];
					if(v2.contains(v1)){
						if(!m_edges.containsKey(v1.index)){
							m_edges.put(v1.index, (new Vector<Integer>()));
						}
						
						addVertex(m_edges.get(v1.index), v2.index);	
					}
				}
			}
		}

		//outputEdges();
	}
	
	/**Output the 'm_edges'
	 * 
	 */
	private void outputEdges(){
		Iterator<Integer> itr = m_edges.keySet().iterator();
		while(itr.hasNext()){
			Integer index = itr.next();
			System.out.println(m_cuboids[index].headname);
			
			Vector<Integer> vec = m_edges.get(index);
			String chain = new String("");
			for(int i=0;i<vec.size();i++){
				chain += m_cuboids[vec.get(i)].headname+", ";
			}
			System.out.println("    "+chain);
			
		}
	}
	
	/**Output the 'm_levels'
	 * 
	 */
	private void outputLevels(){
		for(int i=0;i<m_levels.length;i++){
			String title = new String();
			for(int j = 0;j<m_levels[i].size();j++)
			{
				VERTEX v = m_cuboids[m_levels[i].get(j)];
				title += ",  "+v.headname +"("+v.weight+")"+"["+v.card+"]";
			}
			System.out.println(i+": "+title);
		}
		System.out.println("***********levels***********");
	}
	
	/**Output the all chain names
	 * 
	 */
	private void outputChains(){
		for(int i=0;i<m_levels.length;i++){
			String title = new String();
			for(int j = 0;j<m_levels[i].size();j++){
				VERTEX v = m_cuboids[m_levels[i].get(j)];
				System.out.println(v.chainname);
			}
		}
		System.out.println("***********chains***********");
	}

	
	/**Combine the 'm_cuboids' which are in the same chain and adjust their name's order
	 * 
	 */
	private void combine(){
		for(int l=0; l < m_levels.length-1; l++){	
			Vector<Integer> vec1 = m_levels[l];
			
			for(int i=0;i<vec1.size();i++){
				VERTEX v1 = m_cuboids[vec1.get(i)];
				
				if(!v1.isLowCard()){
					Vector<Integer> vec2 = m_edges.get(v1.index);
					
					for(int j=0;j<vec2.size();j++){
						VERTEX v2 = m_cuboids[vec2.get(j)];
						if(!v2.used){
							v2.chainname += m_chainSperator + v1.chainname;
							v2.used = true;
							//delete v1 from L
							vec1.remove(i);
							i--;
							break;
						}
					}
				}	
			}
		}
		
//		outputLevels();
//		outputChains();
		
		//adjust chain name
		for(int i=0;i<m_levels.length;i++){
			for(int j = 0;j<m_levels[i].size();j++){
				VERTEX v = m_cuboids[m_levels[i].get(j)];
				v.adjustChainName();
			}
		}
		//outputChains();
	}
	
	/**Generate all batches
	 * 
	 */
	private void genBatches(){
		//In order to NOT contain "*", so 'i' is from 1 
		for(int i=1;i<m_levels.length;i++){
			
			for(int j = 0;j<m_levels[i].size();j++){
				VERTEX v = m_cuboids[m_levels[i].get(j)];
				StringTokenizer itr = new StringTokenizer(v.chainname, m_chainSperator);
				Vector<String> vec = new Vector<String>();
				
				while(itr.hasMoreTokens()){
					vec.add(0, itr.nextToken());
				}
				m_batchSet.add(0, vec);
			}
			
		}
		
		//outputBatches();
	}
	
	/** Get batch's info in CUBOID structure;
	 * @return
	 */
	public Vector<BATCHINFO> getBatchExt(){
		Vector<BATCHINFO> vecBatchInfo = new Vector<BATCHINFO>();
		
		Iterator<Vector<String>> itr1 = m_batchSet.iterator();
		int batchId = 0;
		while(itr1.hasNext()){
			Vector<String> vec = itr1.next();
			String headCubodiName = vec.get(vec.size()-1);
			
			VERTEX v = getCuboidByName(headCubodiName);
			BATCHINFO bi = new BATCHINFO(batchId, headCubodiName, v.chainname, v.level, v.weight, v.card);
			vecBatchInfo.add(bi);
			batchId++;
		}
		
		return vecBatchInfo;
	}
	
	/** Get VERTEX by its name
	 * @param name
	 * @return
	 */
	private VERTEX getCuboidByName(String name) {
		for(int i=0;i<m_cuboids.length;i++){
			if(m_cuboids[i].nameEquals(name)){
				return m_cuboids[i];
			}
		}
		
		return null;
	}

	/**output all Batches
	 * 
	 */
	private void outputBatches() {
		for (int i = 0; i < m_batchSet.size(); i++) {
			System.out.println(m_batchSet.get(i));
		}
		
		System.out.println("***********batches***********");
	}
}

