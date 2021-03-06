package org.apache.giraph.comm.messages;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static org.apache.giraph.conf.GiraphConstants.MAX_PARTITIONS_IN_MEMORY;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.requests.SendWorkerMessagesRequest;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.DiskBackedOnlineComputePartitionStore;
import org.apache.giraph.partition.DiskBackedPartitionStore;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.partition.PartitionStore;
import org.apache.giraph.utils.ByteArrayVertexIdData;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.EmptyIterable;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.hdfs.server.namenode.FileChecksumServlets.GetServlet;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

/**
 * Message store which separates data by partitions, and submits them to
 * underlying message store.
 * 
 * @param <I>
 *            Vertex id
 * @param <V>
 *            Vertex data
 * @param <E>
 *            Edge data
 * @param <M>
 *            Message data
 */
public class DiskBackedOnlineComputeMessageStoreByPartition<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable>
		implements MessageStoreByPartition<I, M> {
	private static final Logger LOG = Logger.getLogger(BspServiceWorker.class);	
	/** Service worker */
	private final CentralizedServiceWorker<I, V, E, M> service;
	ImmutableClassesGiraphConfiguration conf;
	/** Number of messages to keep in memory */
	private final int maxSizeOfMessagesInMemory;
	/** Factory for creating file stores when flushing */
	private final MessageStoreFactory<I, M, FlushableMessageStore<I, M>> fileStoreFactory;
	/** Map from partition id to its message store */
	private final ConcurrentMap<Integer, FlushableMessageStore<I, M>> partitionMessageStores;

	/**
	 * @param service
	 *            Service worker
	 * @param maxSizeOfMessagesInMemory
	 *            Number of messages to keep in memory
	 * @param fileStoreFactory
	 *            Factory for creating file stores when flushing
	 */
	public DiskBackedOnlineComputeMessageStoreByPartition(
			CentralizedServiceWorker<I, V, E, M> service,
			int maxSizeOfMessagesInMemory,
			MessageStoreFactory<I, M, FlushableMessageStore<I, M>> fileStoreFactory) {
		this.service = service;
		this.maxSizeOfMessagesInMemory = maxSizeOfMessagesInMemory;
		this.fileStoreFactory = fileStoreFactory;
		partitionMessageStores = Maps.newConcurrentMap();
		conf = ((BspServiceWorker)service).getConfiguration();
		useOnlineCompute = conf.useHotPartition();
	}
	public long totalSizeFlushedToDisk;
	public long totalSizeOnlineComputed;
	public void addRePartitionMessages(ByteArrayVertexIdMessages<I, M> messages){
		ByteArrayVertexIdMessages<I, M>.VertexIdMessageBytesIterator vertexIdMessageBytesIterator = messages
				.getVertexIdMessageBytesIterator();
		// Try to copy the message buffer over rather than
		// doing a deserialization of a message just to know its size. This
		// should be more efficient for complex objects where serialization is
		// expensive. If this type of iterator is not available, fall back to
		// deserializing/serializing the messages
		DiskBackedOnlineComputePartitionStore<I, V, E, M> pStore = 
				(DiskBackedOnlineComputePartitionStore<I, V, E, M>)service.getPartitionStore();
		int partitionNum = pStore.getNumPartitions();
		ByteArrayVertexIdMessages<I, M>[] msgCache = new ByteArrayVertexIdMessages[partitionNum];
		if (vertexIdMessageBytesIterator != null) {
			while (vertexIdMessageBytesIterator.hasNext()) {
				vertexIdMessageBytesIterator.next();
				I vid = vertexIdMessageBytesIterator
								.getCurrentVertexId();
				M msg = vertexIdMessageBytesIterator.getCurrentData();
				
				int partitionId = pStore.getRePartitionId(vid);
				ByteArrayVertexIdMessages<I, M> p_msg = msgCache[partitionId];
			    if (p_msg == null) {
			      p_msg = new ByteArrayVertexIdMessages<I, M>();
			      p_msg.setConf(conf);
			      p_msg.initialize();
			      msgCache[partitionId] = p_msg;
			    } 
			    p_msg.add(vid, msg);
			}
		}else {
			ByteArrayVertexIdMessages<I, M>.VertexIdMessageIterator vertexIdMessageIterator = messages
					.getVertexIdMessageIterator();
			while (vertexIdMessageIterator.hasNext()) {
				vertexIdMessageIterator.next();
				I vid = vertexIdMessageIterator.getCurrentVertexId();
				M msg = vertexIdMessageIterator.getCurrentMessage();
				int partitionId = pStore.getRePartitionId(vid);
				ByteArrayVertexIdMessages<I, M> p_msg = msgCache[partitionId];
			    if (p_msg == null) {
			      p_msg = new ByteArrayVertexIdMessages<I, M>();
			      p_msg.setConf(conf);
			      p_msg.initialize();
			      msgCache[partitionId] = p_msg;
			    } 
			    p_msg.add(vid, msg);
			}
		}
		for(int i=0; i<partitionNum; i++){
			if(msgCache[i]!=null)
				try {
					addPartitionMessagesInternal(i, msgCache[i]);
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}
	boolean useOnlineCompute = false;
	public void addPartitionMessages(int partitionId,
			ByteArrayVertexIdMessages<I, M> messages) throws IOException {
		if(useOnlineCompute)
			addRePartitionMessages(messages);
		else
			addPartitionMessagesInternal(partitionId, messages);
	}
	public void addPartitionMessagesInternal(int partitionId,
			ByteArrayVertexIdMessages<I, M> messages) throws IOException {
		Partition<I, V, E, M> partition = 
				getPartitionIfInMemory(partitionId);

		int size = messages.getSize();
		if(partition==null){
//			Log.info("partition "+partitionId+"not in memory, about to append this msg into disk");
			addPartitionMessagesToDisk(partitionId, messages);
			totalSizeFlushedToDisk += size;
		}else{
			totalSizeOnlineComputed += size;
			ByteArrayVertexIdMessages<I, M>.VertexIdMessageBytesIterator vertexIdMessageBytesIterator = messages
					.getVertexIdMessageBytesIterator();
			// Try to copy the message buffer over rather than
			// doing a deserialization of a message just to know its size. This
			// should be more efficient for complex objects where serialization is
			// expensive. If this type of iterator is not available, fall back to
			// deserializing/serializing the messages
			if (vertexIdMessageBytesIterator != null) {
				while (vertexIdMessageBytesIterator.hasNext()) {
					vertexIdMessageBytesIterator.next();
					Vertex<I, ?, ?, M> vertex = partition
							.getVertex(vertexIdMessageBytesIterator
									.getCurrentVertexId());
					vertex.onlineCompute(vertexIdMessageBytesIterator
							.getCurrentData(), service.getSuperstep());
				}
			} else {
				ByteArrayVertexIdMessages<I, M>.VertexIdMessageIterator vertexIdMessageIterator = messages
						.getVertexIdMessageIterator();
				while (vertexIdMessageIterator.hasNext()) {
					vertexIdMessageIterator.next();
					Vertex<I, ?, ?, M> vertex = partition
							.getVertex(vertexIdMessageIterator.getCurrentVertexId());
					vertex.onlineCompute(vertexIdMessageIterator
							.getCurrentData(), service.getSuperstep());
				}
			}
			putPartition(partition);
		}
	}
	//TODO: to be optimized
	public void addPartitionMessagesToDisk(int partitionId,
			ByteArrayVertexIdMessages<I, M> messages) throws IOException {
		FlushableMessageStore<I, M> flushableMessageStore = getMessageStore(partitionId);
		if (flushableMessageStore instanceof DiskBackedMessageStore) {
			DiskBackedMessageStore<I, M> diskBackedMessageStore = (DiskBackedMessageStore<I, M>) flushableMessageStore;
			ByteArrayVertexIdMessages<I, M>.VertexIdMessageIterator vertexIdMessageIterator = messages
					.getVertexIdMessageIterator();
			while (vertexIdMessageIterator.hasNext()) {
				vertexIdMessageIterator.next();
				boolean ownsVertexId = diskBackedMessageStore
						.addVertexMessages(vertexIdMessageIterator
								.getCurrentVertexId(), Collections
								.singleton(vertexIdMessageIterator
										.getCurrentMessage()));
				if (ownsVertexId) {
					vertexIdMessageIterator.releaseCurrentVertexId();
				}
			}
		} else {
			throw new IllegalStateException(
					"addPartitionMessages: Doesn't support " + "class "
							+ flushableMessageStore.getClass());
		}
		checkMemory();
	}

	@Override
	public void addMessages(MessageStore<I, M> messageStore) throws IOException {
		for (I destinationVertex : messageStore.getDestinationVertices()) {
			FlushableMessageStore<I, M> flushableMessageStore = getMessageStore(destinationVertex);
			if (flushableMessageStore instanceof DiskBackedMessageStore) {
				DiskBackedMessageStore<I, M> diskBackedMessageStore = (DiskBackedMessageStore<I, M>) flushableMessageStore;
				Iterable<M> messages = messageStore
						.getVertexMessages(destinationVertex);
				diskBackedMessageStore.addVertexMessages(destinationVertex,
						messages);
			} else {
				throw new IllegalStateException("addMessages: Doesn't support "
						+ "class " + flushableMessageStore.getClass());
			}
		}
		checkMemory();
	}

	@Override
	public Iterable<M> getVertexMessages(I vertexId) throws IOException {
		if (hasMessagesForVertex(vertexId)) {
			return getMessageStore(vertexId).getVertexMessages(vertexId);
		} else {
			return EmptyIterable.get();
		}
	}

	@Override
	public int getNumberOfMessages() {
		int numOfMessages = 0;
		for (FlushableMessageStore<I, M> messageStore : partitionMessageStores
				.values()) {
			numOfMessages += messageStore.getNumberOfMessages();
		}
		return numOfMessages;
	}

	@Override
	public boolean hasMessagesForVertex(I vertexId) {
		return getMessageStore(vertexId).hasMessagesForVertex(vertexId);
	}

	@Override
	public Iterable<I> getDestinationVertices() {
		List<I> vertices = Lists.newArrayList();
		for (FlushableMessageStore<I, M> messageStore : partitionMessageStores
				.values()) {
			Iterables.addAll(vertices, messageStore.getDestinationVertices());
		}
		return vertices;
	}

	@Override
	public Iterable<I> getPartitionDestinationVertices(int partitionId) {
		FlushableMessageStore<I, M> messageStore = partitionMessageStores
				.get(partitionId);
		if (messageStore == null) {
			return Collections.emptyList();
		} else {
			return messageStore.getDestinationVertices();
		}
	}

	@Override
	public void clearVertexMessages(I vertexId) throws IOException {
		if (hasMessagesForVertex(vertexId)) {
			getMessageStore(vertexId).clearVertexMessages(vertexId);
		}
	}

	@Override
	public void clearPartition(int partitionId) throws IOException {
		FlushableMessageStore<I, M> messageStore = partitionMessageStores
				.get(partitionId);
		if (messageStore != null) {
			messageStore.clearAll();
		}
	}

	@Override
	public void clearAll() throws IOException {
		for (FlushableMessageStore<I, M> messageStore : partitionMessageStores
				.values()) {
			messageStore.clearAll();
		}
		partitionMessageStores.clear();
	}

	/**
	 * Checks the memory status, 
	 * pin vertex partition and do online compute if necessary
	 * 
	 * @throws IOException
	 */
	private void checkMemory() throws IOException {
		while (memoryFull()) {
//			pinAndCompute();
			flushOnePartition();
		}
	}
	 /**
	   * Finds biggest partition and flushes it to the disk
	   *
	   * @throws IOException
	   */
	  private void flushOnePartition() throws IOException {
	    int maxMessageSize = 0;
	    FlushableMessageStore<I, M> biggestStore = null;
	    for (FlushableMessageStore<I, M> messageStore :
	        partitionMessageStores.values()) {
	      int msgSize = messageStore.getInMemMsgSize();
	      if (msgSize > maxMessageSize) {
	        maxMessageSize = msgSize;
	        biggestStore = messageStore;
	      }
	    }
	    if (biggestStore != null) {
	      biggestStore.flush();
	    }
	  }
	/**
	 * Check if memory is full
	 * 
	 * @return True iff memory is full
	 */
	private boolean memoryFull() {
		int totalMessageSize = 0;
		for (FlushableMessageStore<I, M> messageStore : partitionMessageStores
				.values()) {
			totalMessageSize += messageStore.getInMemMsgSize();
		}
		return totalMessageSize*1.0/1000 > maxSizeOfMessagesInMemory;
	}

	/**
	 * Finds biggest partition and flushes it to the disk
	 * TODO: there are other ways to choose a partition to pin 
	 * @throws IOException
	 */
	private void pinAndCompute() throws IOException {
		int maxMessages = 0;
		int pid = -1;
		FlushableMessageStore<I, M> chosenStore = null;
		for (Entry<Integer, FlushableMessageStore<I, M>> entry : partitionMessageStores
				.entrySet()) {
			FlushableMessageStore<I, M> messageStore = entry.getValue();
			int partitionId = entry.getKey();
			int numMessages = messageStore.getNumberOfMessages();
			if (numMessages > maxMessages) {
				pid = partitionId; 
				maxMessages = numMessages;
				chosenStore = messageStore;
			}
		}
		if (chosenStore != null) {
//			pinPartition(pid);
			computeInMemMessagePartition(chosenStore);
			
		    
//			chosenStore.flush();
		}
	}

//	/**
//	 * pull in a vertex partition from disk to memory if neccessary 
//	 * @param pid
//	 */
//	private void pinPartition(int pid) {
//		if(isInMemory(pid)){
//			return;
//		}else{
//			this.service.getServerData().getPartitionStore().getPartition(pid);
//		}
//	}

	/**
	 * after using the partition, must call putPartition
	 * @param pid
	 * @return
	 */
	private Partition<I,V,E,M> getPartitionIfInMemory(int pid) {
		if(service.getPartitionStore() instanceof DiskBackedOnlineComputePartitionStore){
			DiskBackedOnlineComputePartitionStore<I, V, E, M> pStore = 
					(DiskBackedOnlineComputePartitionStore<I, V, E, M>)service.getPartitionStore();
			return pStore.getPartitionIfIsHot(pid);
		}else{
//			Partition<I,V,E,M> partition = service.getPartitionStore().getPartition(pid);
//			 service.getPartitionStore().putPartition(partition);
//			 return partition;
			return service.getPartitionStore().getPartition(pid);
		}
	}

	/**
	 * must be called after getPartitionIfInMemory 
	 * @param pid
	 * @return
	 */
	private void putPartition(Partition<I,V,E,M> partition) {
		service.getPartitionStore().putPartition(partition);
	}

	
	private void computeInMemMessagePartition(
			FlushableMessageStore<I, M> chosenStore) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * Get message store for partition which holds vertex with required vertex
	 * id
	 * 
	 * @param vertexId
	 *            Id of vertex for which we are asking for message store
	 * @return Requested message store
	 */
	private FlushableMessageStore<I, M> getMessageStore(I vertexId) {
		int partitionId = service.getVertexPartitionOwner(vertexId)
				.getPartitionId();
		return getMessageStore(partitionId);
	}

	/**
	 * Get message store for partition id. It it doesn't exist yet, creates a
	 * new one.
	 * 
	 * @param partitionId
	 *            Id of partition for which we are asking for message store
	 * @return Requested message store
	 */
	private FlushableMessageStore<I, M> getMessageStore(int partitionId) {
		FlushableMessageStore<I, M> messageStore = partitionMessageStores
				.get(partitionId);
		if (messageStore != null) {
			return messageStore;
		}
		messageStore = fileStoreFactory.newStore();
		messageStore.setPartitionId(partitionId);
		FlushableMessageStore<I, M> store = partitionMessageStores.putIfAbsent(
				partitionId, messageStore);
		return (store == null) ? messageStore : store;
	}
	public Iterable<IMWritable<I,M>> getPartitionMessages(int pid){
		DiskBackedMessageStore<I, M> st = (DiskBackedMessageStore<I,M>)getMessageStore(pid);
		return st.getPartitionMessages();
	}
	@Override
	public void writePartition(DataOutput out, int partitionId)
			throws IOException {
		FlushableMessageStore<I, M> partitionStore = partitionMessageStores
				.get(partitionId);
		out.writeBoolean(partitionStore != null);
		if (partitionStore != null) {
			partitionStore.write(out);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(partitionMessageStores.size());
		for (Entry<Integer, FlushableMessageStore<I, M>> entry : partitionMessageStores
				.entrySet()) {
			out.writeInt(entry.getKey());
			entry.getValue().write(out);
		}
	}

	@Override
	public void readFieldsForPartition(DataInput in, int partitionId)
			throws IOException {
		if (in.readBoolean()) {
			FlushableMessageStore<I, M> messageStore = fileStoreFactory
					.newStore();
			messageStore.setPartitionId(partitionId);
			messageStore.readFields(in);
			partitionMessageStores.put(partitionId, messageStore);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int numStores = in.readInt();
		for (int s = 0; s < numStores; s++) {
			int partitionId = in.readInt();
			FlushableMessageStore<I, M> messageStore = fileStoreFactory
					.newStore();
			messageStore.setPartitionId(partitionId);
			messageStore.readFields(in);
			partitionMessageStores.put(partitionId, messageStore);
		}
	}

	/**
	 * Create new factory for this message store
	 * 
	 * @param service
	 *            Service worker
	 * @param maxMessagesInMemory
	 *            Number of messages to keep in memory
	 * @param fileStoreFactory
	 *            Factory for creating file stores when flushing
	 * @param <I>
	 *            Vertex id
	 * @param <V>
	 *            Vertex data
	 * @param <E>
	 *            Edge data
	 * @param <M>
	 *            Message data
	 * @return Factory
	 */
	public static <I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable>
	    MessageStoreFactory<I, M, MessageStoreByPartition<I, M>> newFactory(
			CentralizedServiceWorker<I, V, E, M> service,
			int maxMessagesInMemory,
			MessageStoreFactory<I, M, FlushableMessageStore<I, M>> fileStoreFactory) {
		return new Factory<I, V, E, M>(service, maxMessagesInMemory,
				fileStoreFactory);
	}

	/**
	 * Factory for {@link DiskBackedOnlineComputeMessageStoreByPartition}
	 * 
	 * @param <I>
	 *            Vertex id
	 * @param <V>
	 *            Vertex data
	 * @param <E>
	 *            Edge data
	 * @param <M>
	 *            Message data
	 */
	private static class Factory<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable>
			implements MessageStoreFactory<I, M, MessageStoreByPartition<I, M>> {
		/** Service worker */
		private final CentralizedServiceWorker<I, V, E, M> service;
		/** Number of messages to keep in memory */
		private final int maxMessagesInMemory;
		/** Factory for creating file stores when flushing */
		private final MessageStoreFactory<I, M, FlushableMessageStore<I, M>> fileStoreFactory;

		/**
		 * @param service
		 *            Service worker
		 * @param maxMessagesInMemory
		 *            Number of messages to keep in memory
		 * @param fileStoreFactory
		 *            Factory for creating file stores when flushing
		 */
		public Factory(
				CentralizedServiceWorker<I, V, E, M> service,
				int maxMessagesInMemory,
				MessageStoreFactory<I, M, FlushableMessageStore<I, M>> fileStoreFactory) {
			this.service = service;
			this.maxMessagesInMemory = maxMessagesInMemory;
			this.fileStoreFactory = fileStoreFactory;
		}

		@Override
		public MessageStoreByPartition<I, M> newStore() {
			return new DiskBackedOnlineComputeMessageStoreByPartition<I, V, E, M>(service,
					maxMessagesInMemory, fileStoreFactory);
		}
	}

	@Override
	public void addIncomePartitionMessages(int partitionId,
			ByteArrayVertexIdMessages<I, M> messages) throws IOException {
		throw new UnsupportedOperationException(
				"disk based do not support addIncomePartitionMessages");
	}

}
