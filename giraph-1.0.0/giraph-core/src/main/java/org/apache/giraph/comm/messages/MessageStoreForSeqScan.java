package org.apache.giraph.comm.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.MapMaker;

public class MessageStoreForSeqScan<I extends WritableComparable, M extends Writable, T>
		implements MessageStoreByPartition<I, M> {

	/** Service worker */
	protected final CentralizedServiceWorker<I, ?, ?, M> service;
	/** Map from partition id to map from vertex id to messages for that vertex */
	protected final ConcurrentMap<Integer, ConcurrentMap<I, T>> map;
	/** Giraph configuration */
	protected final ImmutableClassesGiraphConfiguration<I, ?, ?, M> config;

	/**
	 * Constructor
	 * 
	 * @param service
	 *            Service worker
	 * @param config
	 *            Giraph configuration
	 */
	public MessageStoreForSeqScan(CentralizedServiceWorker<I, ?, ?, M> service,
			ImmutableClassesGiraphConfiguration<I, ?, ?, M> config) {
		this.service = service;
		this.config = config;
		map = new MapMaker().concurrencyLevel(
				config.getNettyServerExecutionConcurrency()).makeMap();
	}

	@Override
	public int getNumberOfMessages() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean hasMessagesForVertex(I vertexId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Iterable<I> getDestinationVertices() {
		// TODO Auto-generated method stub
		return null;
	}
	// TODO Auto-generated method stub

	@Override
	public void addMessages(MessageStore<I, M> messageStore) throws IOException {
		// not supported 
	}

	@Override
	public Iterable<M> getVertexMessages(I vertexId) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clearVertexMessages(I vertexId) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearAll() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	
	@Override
	public void addPartitionMessages(int partitionId,
			ByteArrayVertexIdMessages<I, M> messages) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void addIncomePartitionMessages(int partitionId,
			ByteArrayVertexIdMessages<I, M> messages) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public Iterable<I> getPartitionDestinationVertices(int partitionId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clearPartition(int partitionId) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void writePartition(DataOutput out, int partitionId)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void readFieldsForPartition(DataInput in, int partitionId)
			throws IOException {
		// TODO Auto-generated method stub

	}

}
