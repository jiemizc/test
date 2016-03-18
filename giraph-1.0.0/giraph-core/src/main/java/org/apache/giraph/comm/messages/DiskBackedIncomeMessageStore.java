package org.apache.giraph.comm.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class DiskBackedIncomeMessageStore<I extends WritableComparable, M extends Writable>
		implements FlushableMessageStore<I, M> {
	/**
	 * In-memory message map (must be sorted to insure that the ids are ordered)
	 */
	private volatile List<InMemMessagePair<I>> inMemoryMessages;
	/** Hadoop configuration */
	private final ImmutableClassesGiraphConfiguration<I, ?, ?, M> config;
	/** Counter for number of messages in-memory */
	private final AtomicInteger numberOfMessagesInMemory;
	/** File stores in which we keep flushed messages */
	private final List<IncomeFileMessageStore<I, M>> fileStores;
	/** Factory for creating file stores when flushing */
	private final MessageStoreFactory<I, M, BasicMessageStore<I, M>> fileStoreFactory;
	/** Lock for disk flushing */
	private final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

	public DiskBackedIncomeMessageStore(
			ImmutableClassesGiraphConfiguration<I, ?, ?, M> config,
			MessageStoreFactory<I, M, BasicMessageStore<I, M>> fileStoreFactory) {
		inMemoryMessages = new ArrayList<InMemMessagePair<I>>();
		this.config = config;
		numberOfMessagesInMemory = new AtomicInteger(0);
		fileStores = Lists.newArrayList();
		this.fileStoreFactory = fileStoreFactory;
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
		// no need to do this in incoming mode
		return null;
	}

	@Override
	public void addMessages(MessageStore<I, M> messageStore) throws IOException {
		// TODO Auto-generated method stub

	}
	
	public Iterator<IncomeFileMessageStore<I, M>> getMessageStoreIterator(){
		return fileStores.iterator();
	}
	public List<IncomeFileMessageStore<I, M>> getMessageStoreList(){
		return fileStores;
	}
	public List<InMemMessagePair<I>> getInMemMessageStore(){
		return inMemoryMessages;
	}
	@Override
	public Iterable<M> getVertexMessages(I vertexId) throws IOException {
		// TODO Auto-generated method stub
		// no need to do this in incoming mode
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
		// TODO Auto-generated method stub(DiskBackedIncomeMessageStore)

	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void flush() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public int getPartitionId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setPartitionId(int partitionId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getInMemMsgSize() {
		// TODO Auto-generated method stub
		return 0;
	}
}
