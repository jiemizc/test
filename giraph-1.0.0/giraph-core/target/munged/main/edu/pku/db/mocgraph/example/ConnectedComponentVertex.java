package edu.pku.db.mocgraph.example;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;


public class ConnectedComponentVertex extends
		Vertex<IntWritable, IntWritable, NullWritable, IntWritable> {

	@Override
	public void computeSingleMsg(IntWritable value,
			IntWritable message, long superstep) {
		if (value.get() > message.get()) {
			value.set(message.get());
			wakeUp();
		}
	}

	@Override
	public void sendMessages(IntWritable value, long superstep) {
		sendMessageToAllEdges(value);
		voteToHalt();
	}

	/**
	 * only used for synchronous mode
	 */
	@Override
	public IntWritable createNewValue(IntWritable old, long superstep){
		return new IntWritable(old.get());
	}
	
	@Override
	public void compute(Iterable<IntWritable> messages) throws IOException {
		if (getSuperstep() == 0) {
			sendMessageToAllEdges(this.getValue());
			return;
		}

		boolean changed = false;

		if (getSuperstep() >= 1) {
			for (IntWritable message : messages) {
				if (this.getValue().get() > message.get()) {
					this.getValue().set(message.get());
					changed = true;
				}
			}
		}

		if (changed) {
			sendMessageToAllEdges(this.getValue());
		} else {
			voteToHalt();
		}
	}

}
