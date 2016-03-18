package org.apache.giraph.graph;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public abstract class IncomingModeVertex<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable>
 extends Vertex<I, V, E, M> {

	@Override
	public void compute(Iterable<M> messages) throws IOException {
	}
	
	public abstract void computeIncomingMode();

}
