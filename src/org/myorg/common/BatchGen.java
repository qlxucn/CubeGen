/*
 * The final result of cuboids are stored in 'm_batchSet' as 
 * [3_, 3_2_, 3_2_1_, 3_2_1_0_]
 * [0_, 0_3_, 0_3_2_]
 * [1_, 1_3_, 1_3_0_]
 * [2_, 2_1_, 2_1_0_]
 * [2_0_]
 * [1_0_]
 * */
package org.myorg.common;

import java.util.Iterator;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.Vector;


public class BatchGen {
	private int m_eleNum;// Number of elements
	private int m_count;// Number of cuboids
	private Vector<String>[] m_cuboids;// 数组脚标为元素个数，将相同元素个数的name放在一个vector中
	private Vector<Vector<String>> m_batchSet = new Vector<Vector<String>>();// 不包括空集的batch
	private Stack<String> m_stackBak = new Stack<String>();
	private Stack<String> m_stackTmp = new Stack<String>();
	public final String m_nameSperator = Common.nameSeperator;

	public int m_statCount = 0;// 统计运行时间

	@SuppressWarnings("unchecked")
	public void create(int eleNum) {
		m_eleNum = eleNum;
		m_count = (int) Math.pow(2, m_eleNum);// Include the 'null set'
		m_cuboids = (Vector<String>[]) new Vector[eleNum + 1];
		for (int i = 0; i < m_eleNum + 1; i++) {
			m_cuboids[i] = new Vector<String>();
		}
		// Generate the cuboids
		genCuboids();

		// Generate the batch
		genAll();
	}

	private void genAll() {
		// 从元素个数最多的cuboid开始寻找
		for (int num = m_eleNum; num > 0; num--) {
			genSub(num);// Generate the batches for 'eleNum' elements
		}
	}

	private void genSub(int num) {
		String name = new String();

		while (true) {
			if (m_cuboids[num].size() != 0) {
				name = m_cuboids[num].firstElement();

				m_stackBak.clear();
				m_stackTmp.clear();
				// 找出最长cuboids集合，存入m_stackBak中
				pushSub(name, num);
				// 同时会删除相应的cuboid
				spillStack(m_stackBak);
			} else {
				return;
			}

		}
	}

	private void pushSub(String name, int n) {
		// name contains 'n' elements
		m_stackTmp.push(name);

		// find n-1 sub set
		n--;
		if (n > 0) {
			for (int i = 0; i < m_cuboids[n].size(); i++) {
				m_statCount++;
				String subName = m_cuboids[n].get(i);
				if (isInclued(name, subName)) {
					pushSub(subName, n);
				} else {
					// noop
				}
			}

		}

		if (m_stackTmp.size() > m_stackBak.size()) {
			m_stackBak = (Stack<String>) m_stackTmp.clone();
		}

		m_stackTmp.pop();
	}

	private void spillStack(Stack<String> stack) {
		Vector<String> batch = new Vector<String>();

		// get the cuboids from the stack
		String name = stack.pop();
		removeFromCuboids(name);
		batch.add(name);

		// System.out.print(name + ", ");

		while (!stack.isEmpty()) {
			String nameExt = stack.pop();
			removeFromCuboids(nameExt);

			name = adjustNameExt(name, nameExt);
			batch.add(name);

			// System.out.print(name + ", ");
		}

		// System.out.println("");

		m_batchSet.add(batch);
	}

	private void removeFromCuboids(String name) {
		for (int i = 0; i < m_cuboids.length; i++) {
			Iterator<String> itr = m_cuboids[i].iterator();
			while (itr.hasNext()) {
				String tmp = itr.next();
				if (tmp.equals(name)) {
					itr.remove();
					return;
				}
			}
		}
	}

	// 根据name，调整nameExt的元素顺序，并返回新的nameExt
	// 例如name＝“4_5_”,nameExt＝“2_4_3_5_6_”,则nameExt调整为“4_5_2_3_6_”
	private String adjustNameExt(String name, String nameExt) {
		String regex = "[" + name.replaceAll(m_nameSperator, "") + "]";
		nameExt = name + m_nameSperator + nameExt.replaceAll(regex, "");

		StringTokenizer itr = new StringTokenizer(nameExt, m_nameSperator);
		nameExt = "";
		while (itr.hasMoreTokens()) {
			nameExt += itr.nextToken() + m_nameSperator;
		}

		return nameExt;
	}

	// if 'name' contains 'subName'
	private boolean isInclued(String name, String subName) {
		String[] L = name.split(m_nameSperator);
		String[] subL = subName.split(m_nameSperator);
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

	private void genCuboids() {
		int flag = 0;
		int flagTmp = flag;
		int num;
		String name = new String();

		for (int i = 0; i < m_count; i++) {
			name = "";
			num = 0;
			flagTmp = flag;
			for (int j = m_eleNum; j > 0; j--) {
				if ((flagTmp & 0x01) > 0) {
					name += String.valueOf(j - 1) + m_nameSperator;
					num++;
				}
				flagTmp = flagTmp >>> 1;
			}
			m_cuboids[num].add(name);
			// System.out.println(name);
			flag++;
		}
	}

	public Vector<Vector<String>> getBatch() {
		return m_batchSet;
	}

	public int getBatchCount() {
		return m_batchSet.size();
	}

	private void printCuboids() {
		for (int i = 0; i < m_cuboids.length; i++) {
			System.out.println(m_cuboids[i]);
		}
	}

	private void printBatchGen() {
		for (int i = 0; i < m_batchSet.size(); i++) {
			System.out.println(m_batchSet.get(i));
		}
	}

	// public static void main(String[] args) {
	// int eleNum = 8;
	// BatchGen bg = new BatchGen();
	// bg.create(eleNum);
	// System.out.println("count = " + String.valueOf(bg.m_statCount));
	// System.out.println("n^n = "
	// + String.valueOf((int) Math.pow(eleNum, eleNum)));
	//
	// bg.printBatchGen();
	//
	// }

}
