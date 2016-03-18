package org.apache.giraph.comm.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import javax.servlet.jsp.jstl.core.Config;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * Problems are: 
 * 1) Priority when multiple threads are reserving memory,  
 *    which partition should be computed and give away first? 
 *    Should first implement FIFO as default. 
 * 2) What if memory released after partitions have already started flushing into
 *    disk. Need come up with a better way to overlap IO and CPU 
 * 3) Should modify the synchronization part of the superstep and vertex should 
 *    broadcast their value until finished computing all its incoming msgs.
 *    So we need to modify the NettyWorkerClient to let this happen: one vertex
 * 
 * 4) Partitions may be accross multiple WritableRequest
 * 
 * @author zhouchang
 * 
 * @param <I>
 * @param <V>
 * @param <E>
 * @param <M>
 */
public class DiskBackedIncomeMessageStoreByPartition<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable>
		implements MessageStoreByPartition<I, M> {

	private final CentralizedServiceWorker<I, V, E, M> service;
	/** Number of messages to keep in memory */
	private final int maxNumberOfMessagesInMemory;
	/** Factory for creating file stores when flushing */
	private final MessageStoreFactory<I, M, FlushableMessageStore<I, M>> fileStoreFactory;
	/** Map from partition id to its message store */
	private final ConcurrentMap<Integer, DiskBackedIncomeMessageStore<I, M>> partitionMessageStores;

	public DiskBackedIncomeMessageStoreByPartition(
			CentralizedServiceWorker<I, V, E, M> service,
			int maxNumberOfMessagesInMemory,
			MessageStoreFactory<I, M, FlushableMessageStore<I, M>> fileStoreFactory) {
		this.service = service;
		this.maxNumberOfMessagesInMemory = maxNumberOfMessagesInMemory;
		this.fileStoreFactory = fileStoreFactory;
		partitionMessageStores = Maps.newConcurrentMap();
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

	@Override
	public void addMessages(MessageStore<I, M> messageStore) throws IOException {
		// TODO Auto-generated method stub

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
		ByteArrayVertexIdMessages<I, M>.VertexIdMessageIterator vertexIdMessageIterator = messages
				.getVertexIdMessageIterator();
		// 1. reserve a memory for these messages.
//		Partition
		// 2. if not enough memory, store it into the disk.
		// else send it to the vertices to be computed asynchronously.
		// Once done computing, the taken memory will be released by that
		// thread.

		Map<I, ?> partitionTable = service.getServerData().getEdgeStore()
				.getOutEdgePartition(partitionId);
		while (vertexIdMessageIterator.hasNext()) {
			vertexIdMessageIterator.next();
//			writeIncomeMessage(partitionTable, vertexIdMessageIterator);
		}
	}

	public Iterator<IncomeFileMessageStore<I, M>> getDiskMessageStoreIterator(Integer partitionId){
		return partitionMessageStores.get(partitionId).getMessageStoreIterator();
	}
	public List<InMemMessagePair<I>> getInMessageStore(Integer partitionId){
		return partitionMessageStores.get(partitionId).getInMemMessageStore();
	}
	public List<IncomeFileMessageStore<I, M>> getDiskMessageStoreList(){
		List<IncomeFileMessageStore<I, M>> stores = 
				new ArrayList<IncomeFileMessageStore<I, M>>();
		for(Entry<Integer,DiskBackedIncomeMessageStore<I, M>> entry:
			partitionMessageStores.entrySet()){
			stores.addAll(entry.getValue().getMessageStoreList());
		}
		return stores;
	}
	public List<List<InMemMessagePair<I>>> getInMessageStoreList(){
		List<List<InMemMessagePair<I>>> stores = 
				new ArrayList<List<InMemMessagePair<I>>>();
		for(Entry<Integer,DiskBackedIncomeMessageStore<I, M>> entry:
			partitionMessageStores.entrySet()){
			stores.add(entry.getValue().getInMemMessageStore());
		}
		return stores;
	}
	@Override
	public Iterable<I> getPartitionDestinationVertices(int partitionId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clearPartition(int partitionId) throws IOException {
		// TODO Auto-generated method stubgetNext

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
