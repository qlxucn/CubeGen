package org.myorg.improve_naive;

import java.io.IOException;
import java.util.BitSet;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.myorg.common.Common;
import org.myorg.common.Global;
import org.myorg.common.Global.TAG_CUBOIDS_RANGE_WEIGHT;

public class ImproveNaiveMapper extends MapReduceBase
		implements
			Mapper<Object, Text, Text, Text> {

	private Global m_gobal = new Global();
	private int ms_splitNum = -1;

	private String m_sParts[] = null;
	private Text m_outKey = new Text();
	private Text m_outValue;

	@Override
	public void configure(JobConf jobConf) {
		m_gobal.initGlobal(jobConf);
		ms_splitNum = m_gobal.nSplitNum;
		m_sParts = new String[ms_splitNum];
		m_outValue = new Text();
	}

	private int index;
	private StringTokenizer itr = null;
	private int realValue;

	public void map(Object key, Text value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {

		// Split the key
		itr = new StringTokenizer(value.toString(), Common.splitSeperator);
		if (itr.countTokens() != ms_splitNum) {
			System.out.println("The number of tokens is NOT correct.");
			return;
		}

		index = 0;
		while (itr.hasMoreTokens() && index < ms_splitNum) {
			m_sParts[index++] = itr.nextToken();
		}

		realValue = Integer.valueOf(m_sParts[ms_splitNum - 1]);
		// Generate the intermediate data for each cuboid
		for(int cuboid:m_gobal.cubeInfoByCuboId.keySet()){
			m_outKey.set(getKeyByName(m_gobal.cubeInfoByCuboId.get(cuboid).name, m_sParts));
			m_outValue.set(String.valueOf(realValue) + Common.value_batchidSeperator
					+ String.valueOf(cuboid));

			output.collect(m_outKey, m_outValue);
		}
	}

	int col;
	StringTokenizer itr2 = null;
	String m_tmpkey = new String();
	
	private String getKeyByName(String name, String[] keyParts) {
		m_tmpkey = "";

		itr2 = new StringTokenizer(name.toString(), Common.nameSeperator);
		while (itr2.hasMoreTokens()) {
			col = Integer.valueOf(itr2.nextToken());
			m_tmpkey += keyParts[col] + Common.splitSeperator;
		}

		return m_tmpkey;
	}
}
