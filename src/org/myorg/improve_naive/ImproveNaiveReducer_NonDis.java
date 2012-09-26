package org.myorg.improve_naive;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.myorg.common.Common;
import org.myorg.common.Global;

public class ImproveNaiveReducer_NonDis extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
	private Global m_global = new Global();
	private boolean m_bInited = false;
	private String m_name = new String();

	@Override
	public void configure(JobConf jobConf) {
		m_global.initGlobal(jobConf);
	}

	private Vector<Integer> vecVal = new Vector<Integer>();
	private Text valueOut = new Text();
	private Text keyOut = new Text();
	private String value_cuboid = new String();
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		if (!values.hasNext()) {
			System.out.println(key.toString() + "= No values");
			return;
		}
		

		vecVal.clear();
		while (values.hasNext()) {
			value_cuboid = values.next().toString();
			vecVal.add(m_global.getFirstHalf(value_cuboid));
		}

		if (!m_bInited) {
			int cuboid = m_global.getLastHalf(value_cuboid);
			m_name = m_global.cubeInfoByCuboId.get(cuboid).name;
			m_bInited = true;
			
			System.out.println("ImproveNaiveReducer.reduce():cuboid="+cuboid+" m_name="+m_name);
		}

		keyOut.set(m_name);
		valueOut.set(key.toString() + Common.splitSeperator
				+ String.valueOf(this.getMedium(vecVal)));

		output.collect(keyOut, valueOut);
	}
	
	private int getMedium(Vector<Integer> vec){
		Collections.sort(vec);
		return vec.get((int) Math.ceil((double)vec.size() / 2) - 1);
	}
}
