package org.apache.giraph.comm.messages;

import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.hadoop.io.WritableComparable;

public class InMemMessagePair<I extends WritableComparable> {
	public I id;
	public ExtendedDataOutput msg;
}
