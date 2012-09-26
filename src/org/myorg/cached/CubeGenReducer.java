/*
 * 一个reducer对应一个batch
 * */

package org.myorg.cached;

import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.myorg.common.Common;
import org.myorg.common.Global;

public class CubeGenReducer extends MapReduceBase
		implements
		Reducer<Text, Text, Text, Text> {

	public class Buffer {
		public String key = null;
		public int value = 0;
	}

	private Global m_global = new Global();
	private boolean m_bInited;
	private OutputCollector<Text, Text> m_localOutput;
	// Buffer of last time. NOTE: m_sBufArry[0] not used, just for convenience
	private Buffer[] m_sBufArry;
	private int m_splitNum;
	private String m_sKeyParts[];
	private int m_nCubeNum;
	private BitSet m_tag;

	private Text m_keyOut = new Text();
	private Text m_valueOut = new Text();

	// private String m_keyTmp = new String();

	@Override
	public void configure(JobConf jobConf) {
		m_global.initGlobal(jobConf);
		m_bInited = false;
		m_sKeyParts = new String[m_global.nSplitNum];
	}

	private String name = new String();
	@Override
	public void close() throws IOException {
		// Output the remaining data
		if (!m_bInited) {
			return;
		}

		for (int i = 1; i < m_sBufArry.length; i++) {
			name = m_global.getCuboidName(m_tag, m_nCubeNum - i);
			// System.out.println("close() : name = " + String.valueOf(name)
			// + " key=" + String.valueOf(m_sBufArry[i]) + " value="
			// + String.valueOf(m_sBufArry[i]) + " i="
			// + String.valueOf(m_sBufArry.length));
			if(null != m_sBufArry[i].key){
				outputCollect(name, m_sBufArry[i].key, m_sBufArry[i].value);
			}
			
		}
		
		m_localOutput.close();
	}

	private int index;
	private int sum;
	private String value_batchid;
	private String keyTmp = new String();
	StringTokenizer itr = null;
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		if (!values.hasNext()) {
			System.out.println(key.toString() + "= No values");
			return;
		}

		// Split the key parts
		itr = new StringTokenizer(key.toString(), Common.splitSeperator);
		index = 0;
		while (itr.hasMoreTokens() && index < m_global.nSplitNum) {
			m_sKeyParts[index++] = itr.nextToken();
		}
		// System.out.println("+++++++reduce(): " + String.valueOf(m_sBufArry)
		// + "  " + String.valueOf(m_gobal));

		sum = 0;
		// Generate the cube whose key is the whole one
		while (values.hasNext()) {
			value_batchid = values.next().toString();
			// System.out.println("Reducer: m_vwValue.getBatchTag()[0] = "+String.valueOf(m_vwValue.getBatchTag()[0]));
			sum += m_global.getFirstHalf(value_batchid);

			if (!m_bInited) {
				initParam(m_global.getLastHalf(value_batchid), key.toString(),
						output);
			}

		}
		// System.out.println("1st: "+String.valueOf(m_tag) +
		// "no="+String.valueOf(m_nCubeNum)+"key = "+key.toString());
		name = m_global.getCuboidName(m_tag, m_nCubeNum);
		outputCollect(name, key.toString(), sum);

		// Generate the output of remaining cubes
		for (int i = 1; i < m_sBufArry.length; i++) {
			keyTmp = genKey(i, m_sKeyParts);
			// System.out.println("m_keyTmp = " + m_keyTmp + " i = "
			// + String.valueOf(i));
			if (keyTmp.equals(m_sBufArry[i].key)) {
				m_sBufArry[i].value += sum;
			} else {
				if (m_sBufArry[i].key != null) {
					// System.out.println("remain: "+String.valueOf(m_tag) +
					// "no="+String.valueOf(m_nCubeNum -
					// i)+"key = "+m_sBufArry[i].key);
					name = m_global.getCuboidName(m_tag, m_nCubeNum - i);
					outputCollect(name, m_sBufArry[i].key, m_sBufArry[i].value);
				}
				m_sBufArry[i].key = keyTmp;
				m_sBufArry[i].value = sum;
			}
		}

	}

	private IntWritable valueTmp = new IntWritable();
	private void outputCollect(String name, String key, int value)
			throws IOException {
		// Combine with old cache date
		m_keyOut.set(key);
		valueTmp.set(value);
		m_localOutput.update(name, m_keyOut, valueTmp);

		// Refresh 'value'
		value = valueTmp.get();

		// collecting
		m_keyOut.set(name);
		m_valueOut.set(key + Common.splitSeperator + String.valueOf(value));
		m_localOutput.collect(m_keyOut, m_valueOut);
	}

	private String keyTmp2 = new String();
	private String genKey(int bufNum, String[] sKeyParts) {
		keyTmp2 = "";
		int nStopPoint = m_splitNum - bufNum;
		for (int i = 0; i < nStopPoint; i++) {
			keyTmp2 += sKeyParts[i] + Common.splitSeperator;
		}

		return keyTmp2;
	}

	private void initParam(int batchId, String key,
			OutputCollector<Text, Text> output) {
		// Initial output buffer
		m_localOutput = output;

		// Initial batch tag
		m_tag = m_global.cubeInfoByBatchId.get(batchId).tag;

		// Initial cube number
		m_nCubeNum = m_global.getCubeNum(m_tag);
		System.out.println("batchId=" + String.valueOf(batchId)
				+ "  m_nCubeNum="
				+ String.valueOf(m_nCubeNum));

		// Initial buffers for the different cuboids
		m_sBufArry = new Buffer[m_nCubeNum];
		for (int i = 0; i < m_sBufArry.length; i++) {
			m_sBufArry[i] = new Buffer();
		}

		// Initial the number of key's parts
		StringTokenizer itr = new StringTokenizer(key, Common.splitSeperator);
		this.m_splitNum = itr.countTokens();

		// Set the initial flag
		this.m_bInited = true;
		// System.out.println("initParam: m_sBufArry[0]"+String.valueOf(m_sBufArry[0]));
	}

}