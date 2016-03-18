package org.apache.giraph.comm.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
 public class IMWritable<ID extends WritableComparable, MSG extends Writable> implements Writable{

	public ID vertexId;
	public MSG message;
	public IMWritable(ImmutableClassesGiraphConfiguration<ID, ?, ?, MSG> conf){
		vertexId = conf.createVertexId();
		message = conf.createMessageValue();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		vertexId.write(out);
		message.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		vertexId.readFields(in);
		message.readFields(in);
	}
	  
  }