package edu.pku.db.mocgraph.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

/**
 * Default single-source shortest paths computation.
 */
public class MultipleLandmarkVertex extends
		Vertex<IntWritable, LandmarkTable, IntWritable, LandmarkTable> {
	/** Source id. */
	public static final String SOURCE_ID = "src";
	/** Default source id. */
	public static final long SOURCE_ID_DEFAULT = 1;
	public static final String PRE = "pre";
	public static final String MARKDIR = "markdir";
	public static final String DEBUGROUND = "supersteps";

	Set<Integer> costMap = null;

	public static class MultipleLandmarkContext extends WorkerContext {
		private static final Logger LOG = Logger
				.getLogger(MultipleLandmarkContext.class);

		public static Set<Integer> landmarks;

		@Override
		public void preApplication() {
			landmarks = new HashSet<Integer>();
			Configuration conf = getContext().getConfiguration();
			BufferedReader br = null;
			try {
				String filePath = conf.get(MARKDIR);
				FileSystem fs = FileSystem.get(URI.create(filePath), conf);
				Path path = new Path(filePath);
				String line;
				br = new BufferedReader(new InputStreamReader(fs.open(path)));
				while ((line = br.readLine()) != null) {
					Integer id = Integer.parseInt(line);
					landmarks.add(id);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				IOUtils.closeStream(br);
			}
		}

		@Override
		public void postApplication() {

		}

		@Override
		public void preSuperstep() {

		}

		@Override
		public void postSuperstep() {
			// TODO Auto-generated method stub

		}

	}

	//
	public void initVertex(LandmarkTable value, long superstep) {
		costMap = new HashSet<Integer>();
		if (superstep == 0) {
			Set<Integer> landmarks = MultipleLandmarkContext.landmarks;
			int key = getId().get();
			for (Integer key1 : landmarks) {
				int initCost;
				int initPre = -1;
				if (!key1.equals(key)) {
					initCost = Integer.MAX_VALUE;
				} else {
					initCost = 0;
					initPre = key;
					costMap.add(key);
				}
				value.put(key1, initPre, initCost);
			}
		}
	}

	//
	// public void initialize(IntWritable id, LandmarkTable value,
	// Iterable<Edge<IntWritable, IntWritable>> edges) {
	// super.initialize(id, value, edges);
	// costMap = new HashSet<Integer>();
	// Set<Integer> landmarks = MultipleLandmarkContext.landmarks;
	// int key = getId().get();
	// for (Integer key1 : landmarks) {
	// int initCost;
	// int initPre = -1;
	// if (!key1.equals(key)) {
	// initCost = Integer.MAX_VALUE;
	// } else {
	// initCost = 0;
	// initPre = key;
	// costMap.add(key);
	// }
	// value.put(key1, initPre, initCost);
	// }
	// }

	/**
	 * mocgraph sendmessages
	 */
	public void sendMessages(LandmarkTable value, long superstep) {
		if (costMap == null)
			initVertex(value, superstep);
		if (!costMap.isEmpty()) {
			for (Edge<IntWritable, IntWritable> edge : getEdges()) {
				LandmarkTable msg = new LandmarkTable();
				for (Integer landmarkid : costMap) {
					int dist = value.getCost(landmarkid);
					int distance = dist + edge.getValue().get();
					msg.put(landmarkid, getId().get(), distance);
				}
				sendMessage(edge.getTargetVertexId(), msg);
			}
			costMap.clear();
		}
		voteToHalt();
	}

	/**
	 * mocgraph onlinecompute
	 */
	public void computeSingleMsg(LandmarkTable value, LandmarkTable msg,
			long superstep) {
		if (costMap == null) {
			initVertex(value, superstep);
		}
		LandmarkTable curLandmarkMap = value;
		// LOG.info("rec msg="+msg.toString());

		// update all the landmark cost
		Set<Entry<Integer, PairIntWritable>> entries = msg.entrySet();
		for (Entry<Integer, PairIntWritable> entry : entries) {
			int landmarkId = entry.getKey();
			int from = entry.getValue().getPre();
			int cost = entry.getValue().getCost();
			int curCost = curLandmarkMap.getCost(landmarkId);
			if (cost < curCost) {
				curLandmarkMap.put(landmarkId, from, cost);
				costMap.add(landmarkId);
			}
		}
		if (!costMap.isEmpty()) {
			wakeUp();
		}
	}

	@Override
	public LandmarkTable createNewValue(LandmarkTable value, long superstep) {
		LandmarkTable rs = new LandmarkTable();
		for (Entry<Integer, PairIntWritable> entry : value.table.entrySet()) {
			rs.put(entry.getKey(), entry.getValue().pre, entry.getValue().cost);
		}
		return rs;
	}

	/**
	 * giraph compute
	 */
	@Override
	public void compute(Iterable<LandmarkTable> msgs)
			throws IOException {
		// <landmark-id, cost> map to update
		if (costMap == null)
			initVertex(getValue(), getSuperstep());
		LandmarkTable curLandmarkMap = getValue();
		for (LandmarkTable msg : msgs) {
			// update all the landmark cost
			Set<Entry<Integer, PairIntWritable>> entries = msg.entrySet();
			for (Entry<Integer, PairIntWritable> entry : entries) {
				int landmarkId = entry.getKey();
				int from = entry.getValue().getPre();
				int cost = entry.getValue().getCost();
				int curCost = curLandmarkMap.getCost(landmarkId);
				if (cost < curCost) {
					curLandmarkMap.put(landmarkId, from, cost);
					costMap.add(landmarkId);
				}
			}
		}
		if (!costMap.isEmpty()) {
			for (Edge<IntWritable, IntWritable> edge : getEdges()) {
				LandmarkTable msg = new LandmarkTable();
				for (Integer landmarkid : costMap) {
					int dist = getValue().getCost(landmarkid);
					int distance = dist + edge.getValue().get();
					msg.put(landmarkid, getId().get(), distance);
				}
				sendMessage(edge.getTargetVertexId(), msg);
			}
			costMap.clear();
		} else
			voteToHalt();
	}
}
