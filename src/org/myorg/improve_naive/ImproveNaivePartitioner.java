package org.myorg.improve_naive;

import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.myorg.common.Common;
import org.myorg.common.Global;
import org.myorg.common.Global.RANGE;

public class ImproveNaivePartitioner implements Partitioner<Text, Text> {
	private Global m_global = new Global();

	@Override
	public void configure(JobConf job) {
		m_global.initGlobal(job);
		Set<Integer> cuboIds = m_global.cubeInfoByCuboId.keySet();
		// Check if "weights.length == cuboIds.size()"
		if (weights.length != cuboIds.size()) {
			System.out
					.println("Error: ImproveNaivePartitioner.configure(): weights.length != cuboIds.size()");
			return;
		}

		int maxId = 0;
		int spanAll = 0;
		for (int id : cuboIds) {
			spans[id] = (int) Math.round(weights[id] * job.getNumReduceTasks());
			spanAll += spans[id];
			// Find the id which has the longest name
			if (m_global.cubeInfoByCuboId.get(id).name.length() > m_global.cubeInfoByCuboId
					.get(maxId).name.length()) {
				maxId = id;
			}
		}
//
//		// print spans
//		for (int id = 0; id < spans.length; id++) {
//			System.out.println("ImproveNaivePartitioner.configure():" +
//					"spans[" + id + "]=" + spans[id]);
//		}
//		System.out
//				.println("ImproveNaivePartitioner.configure():After adjust....");
		// give the remain span to the cuboid which has the longest name
		spans[maxId] += job.getNumReduceTasks() - spanAll;

		// get 'starts'
		for (int id = 0; id < spans.length; id++) {
			if (id == 0) {
				starts[id] = 0;
			} else {
				starts[id] = starts[id - 1] + spans[id - 1];
			}
		}

//		// print 'starts' and 'spans'
//		for (int id = 0; id < spans.length; id++) {
//			System.out.println("ImproveNaivePartitioner.configure():" +
//					"id=" + id + " start=" + starts[id] + "span=" + spans[id]);
//		}

	}
	
	/*weights for 4d*/
	private double[] weights = {
	0.041531823085221145,
	0.06796116504854369,
	0.09398597626752966,
	0.0877831715210356,
	0.050431499460625674,
	0.0703883495145631,
	0.09479503775620281,
	0.034385113268608415,
	0.06216289104638619,
	0.07241100323624595,
	0.057982740021574976,
	0.045307443365695796,
	0.059735706580366775,
	0.06634304207119741,
	0.09479503775620281
	};
	
	private int[] spans = new int[weights.length];
	private int[] starts = new int[weights.length];

	private int cuboid;
	@Override
	public int getPartition(Text key, Text value, int numReduceTask) {
		cuboid = m_global.getLastHalf(value.toString());
		return starts[cuboid] + Math.abs(key.toString().hashCode()) % spans[cuboid];
	}

}

/*weights for 4d*/
//private double[] weights = {
//0.041531823085221145,
//0.06796116504854369,
//0.09398597626752966,
//0.0877831715210356,
//0.050431499460625674,
//0.0703883495145631,
//0.09479503775620281,
//0.034385113268608415,
//0.06216289104638619,
//0.07241100323624595,
//0.057982740021574976,
//0.045307443365695796,
//0.059735706580366775,
//0.06634304207119741,
//0.09479503775620281
//};

/*weights for 3d*/
//private double[] weights = {
//0.083743842364532,
//0.12807881773399,
//0.206896551724138,
//0.103448275862069,
//0.172413793103448,
//0.108374384236453,
//0.197044334975369
//};

/*weights for 5d*/
//private double[] weights = {
//		0.01675086413188,
//		0.019941504918904,
//		0.039085349641053,
//		0.039085349641053,
//		0.043073650624834,
//		0.019143844722148,
//		0.031108747673491,
//		0.034299388460516,
//		0.035097048657272,
//		0.0194097314544,
//		0.035097048657272,
//		0.039085349641053,
//		0.039883009837809,
//		0.017548524328636,
//		0.034565275192768,
//		0.036692369050784,
//		0.039351236373305,
//		0.01675086413188,
//		0.032969954799256,
//		0.035894708854028,
//		0.038553576176549,
//		0.031906407870247,
//		0.034299388460516,
//		0.031906407870247,
//		0.034299388460516,
//		0.035097048657272,
//		0.03749002924754,
//		0.035097048657272,
//		0.038287689444297,
//		0.023929805902686,
//		0.034299388460516
//};


