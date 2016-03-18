package edu.pku.db.mocgraph.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.mortbay.log.Log;

import edu.pku.db.mocgraph.example.writable.IntArrayWritable;

public class TriangleCountingVertex extends
		Vertex<IntWritable, NullWritable, NullWritable, IntArrayWritable> {

	int num = 0;
	IntWritable tmp = new IntWritable();
	LongWritable tNum = new LongWritable(0);

	@Override
	public synchronized void computeSingleMsg(NullWritable value, IntArrayWritable message, long superstep) {
		List<Integer> idToCheck = message.id;
		for (int i = 1; i < idToCheck.size(); i++) {
			int id = idToCheck.get(i);
			tmp.set(id);
			if (getEdgeValue(tmp) != null) {
				num++;
			}
		}
	}

	public synchronized void sendMessages(NullWritable value, long superstep) {
		int id = this.getId().get();
		if (getSuperstep() == 0) {
			for (Edge<IntWritable, NullWritable> e : getEdges()) {
				IntWritable toSend = e.getTargetVertexId();
				int child1 = toSend.get();
				if (child1 < id) {
					List<Integer> msg = new ArrayList<Integer>();
					msg.add(child1);
					for (Edge<IntWritable, NullWritable> e1 : getEdges()) {
						int child2 = e1.getTargetVertexId().get();
						if (child2 < child1)
							msg.add(child2);
					}
					if (msg.size() > 1) {
						IntArrayWritable iaw = new IntArrayWritable(msg);
						this.sendMessage(toSend, iaw);
					}
				}
			}
		} else {
			tNum.set(num);
			this.aggregate("triangleNum", tNum);
			voteToHalt();
		}
	}

	@Override
	public void compute(Iterable<IntArrayWritable> messages) throws IOException {
		if (getSuperstep() == 1) {
			// if(!this.getConf().useOnlineCompute()){
			for (IntArrayWritable message : messages) {
				List<Integer> idToCheck = message.id;
				for (int i = 1; i < idToCheck.size(); i++) {
					int id = idToCheck.get(i);
					tmp.set(id);
					if (getEdgeValue(tmp) != null) {
						num++;
					}
				}
			}
			tNum.set(num);
			getContext().progress();
			this.aggregate("triangleNum", tNum);
			voteToHalt();
		}

		if (getSuperstep() == 0) {
			for (Edge e : getEdges()) {
				IntWritable child1 = (IntWritable) e.getTargetVertexId();
				if (child1.get() < this.getId().get()) {
					List<Integer> msg = new ArrayList<Integer>();
					msg.add(child1.get());
					for (Edge e1 : getEdges()) {
						IntWritable child2 = (IntWritable) e1
								.getTargetVertexId();
						if (child2.get() < child1.get())
							msg.add(child2.get());
					}
					if (msg.size() > 1) {
						IntArrayWritable iaw = new IntArrayWritable(msg);
						this.sendMessage(child1, iaw);
					}
				}
			}
		}
	}
}