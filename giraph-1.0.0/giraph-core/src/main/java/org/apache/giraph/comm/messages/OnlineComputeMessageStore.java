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

package org.apache.giraph.comm.messages;

import com.google.common.collect.Iterators;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionStore;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.RepresentativeByteArrayIterable;
import org.apache.giraph.utils.RepresentativeByteArrayIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * very slow if use outofcore graph, because getPartition() will force partition
 * loading to memory can change this by invoke getpartitionifinmemory()
 * Implementation of {@link SimpleMessageStore} where messages are computed once
 * they arrived.
 *
 * @param <I>
 *            Vertex id
 * @param <M>
 *            Message data
 */
public class OnlineComputeMessageStore<I extends WritableComparable, M extends Writable>
		extends SimpleMessageStore<I, M, ExtendedDataOutput> {

	// TODO: not supported yet
	BlockingQueue<Integer> comingPartition = new LinkedBlockingQueue<Integer>();
	Thread onlineComputeThread = new Thread(new Runnable() {

		@Override
		public void run() {
			Integer pid = null;
			while ((pid = comingPartition.poll()) != null) {
				service.getServerData().getEdgeStore().getDstVidPartition(pid);
			}
		}

	});

	/**
	 * Constructor
	 *
	 * @param service
	 *            Service worker
	 * @param config
	 *            Hadoop configuration
	 */
	public OnlineComputeMessageStore(
			CentralizedServiceWorker<I, ?, ?, M> service,
			ImmutableClassesGiraphConfiguration<I, ?, ?, M> config) {
		super(service, config);
		this.service = service;
		// this.serverData = service.getServerData();
	}

	CentralizedServiceWorker<I, ?, ?, M> service;

	public List<IdMsgPair> blockQ = new ArrayList<IdMsgPair>(50000);

	public class IdMsgPair {
		public I id;
		public int pid;
		public M msg;

		IdMsgPair(int pid, I id, M msg) {
			this.id = id;
			this.pid = pid;
			this.msg = msg;
		}
	}

	@Override
	public void addPartitionMessages(int partitionId,
			ByteArrayVertexIdMessages<I, M> messages) throws IOException {
		PartitionStore<I, ?, ?, M> partitionStore = service.getServerData()
				.getPartitionStore();
		Partition<I, ?, ?, M> partition = partitionStore
				.getPartition(partitionId);
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
				vertex.onlineCompute(
						vertexIdMessageBytesIterator.getCurrentData(),
						service.getSuperstep());

				// blockQ.add(new IdMsgPair<I,M>(partitionId,
				// vertexIdMessageBytesIterator.getCurrentVertexId(),
				// vertexIdMessageBytesIterator.getCurrentData()));
				// System.out.println("added msg");
			}
		} else {
			ByteArrayVertexIdMessages<I, M>.VertexIdMessageIterator vertexIdMessageIterator = messages
					.getVertexIdMessageIterator();
			while (vertexIdMessageIterator.hasNext()) {
				vertexIdMessageIterator.next();
				Vertex<I, ?, ?, M> vertex = partition
						.getVertex(vertexIdMessageIterator.getCurrentVertexId());
				// it could be possible that an unresolved vertex receives a message. 
				if(vertex!=null)
					vertex.onlineCompute(
							vertexIdMessageIterator.getCurrentMessage(),
							service.getSuperstep());
				// ExtendedDataOutput extendedDataOutput =
				// getExtendedDataOutput(partitionMap, vertexIdMessageIterator);
				//
				// synchronized (extendedDataOutput) {
				// vertexIdMessageIterator.getCurrentMessage().write(
				// extendedDataOutput);
				// }
			}
			messages = null;
		}
	}

	@Override
	public void addIncomePartitionMessages(int partitionId,
			ByteArrayVertexIdMessages<I, M> messages) throws IOException {
		// not supported yet
	}

	/**
	 * Special iterable that recycles the message
	 */
	private class MessagesIterable extends RepresentativeByteArrayIterable<M> {
		/**
		 * Constructor
		 *
		 * @param buf
		 *            Buffer
		 * @param off
		 *            Offset to start in the buffer
		 * @param length
		 *            Length of the buffer
		 */
		private MessagesIterable(byte[] buf, int off, int length) {
			super(config, buf, off, length);
		}

		@Override
		protected M createWritable() {
			return config.createMessageValue();
		}
	}

	@Override
	protected Iterable<M> getMessagesAsIterable(
			ExtendedDataOutput extendedDataOutput) {

		return new MessagesIterable(extendedDataOutput.getByteArray(), 0,
				extendedDataOutput.getPos());
	}

	/**
	 * Special iterator only for counting messages
	 */
	private class RepresentativeMessageIterator extends
			RepresentativeByteArrayIterator<M> {
		/**
		 * Constructor
		 *
		 * @param configuration
		 *            Configuration
		 * @param buf
		 *            buffer to read from
		 * @param off
		 *            Offset into the buffer to start from
		 * @param length
		 *            Length of the buffer
		 */
		public RepresentativeMessageIterator(
				ImmutableClassesGiraphConfiguration configuration, byte[] buf,
				int off, int length) {
			super(configuration, buf, off, length);
		}

		@Override
		protected M createWritable() {
			return config.createMessageValue();
		}
	}

	@Override
	protected int getNumberOfMessagesIn(
			ConcurrentMap<I, ExtendedDataOutput> partitionMap) {
		int numberOfMessages = 0;
		for (ExtendedDataOutput extendedDataOutput : partitionMap.values()) {
			numberOfMessages += Iterators
					.size(new RepresentativeMessageIterator(config,
							extendedDataOutput.getByteArray(), 0,
							extendedDataOutput.getPos()));
		}
		return numberOfMessages;
	}

	@Override
	protected void writeMessages(ExtendedDataOutput extendedDataOutput,
			DataOutput out) throws IOException {
		out.writeInt(extendedDataOutput.getPos());
		out.write(extendedDataOutput.getByteArray(), 0,
				extendedDataOutput.getPos());
	}

	@Override
	protected ExtendedDataOutput readFieldsForMessages(DataInput in)
			throws IOException {
		int byteArraySize = in.readInt();
		byte[] messages = new byte[byteArraySize];
		in.readFully(messages);
		ExtendedDataOutput extendedDataOutput = config
				.createExtendedDataOutput(messages, 0);
		return extendedDataOutput;
	}

	/**
	 * Create new factory for this message store
	 *
	 * @param service
	 *            Worker service
	 * @param config
	 *            Hadoop configuration
	 * @param <I>
	 *            Vertex id
	 * @param <M>
	 *            Message data
	 * @return Factory
	 */
	public static <I extends WritableComparable, M extends Writable> MessageStoreFactory<I, M, MessageStoreByPartition<I, M>> newFactory(
			CentralizedServiceWorker<I, ?, ?, M> service,
			ImmutableClassesGiraphConfiguration<I, ?, ?, M> config) {
		return new Factory<I, M>(service, config);
	}

	@Override
	public void addMessages(MessageStore<I, M> messageStore) throws IOException {
		if (messageStore instanceof OnlineComputeMessageStore) {
			OnlineComputeMessageStore<I, M> byteArrayMessagesPerVertexStore = (OnlineComputeMessageStore<I, M>) messageStore;
			for (Map.Entry<Integer, ConcurrentMap<I, ExtendedDataOutput>> partitionEntry : byteArrayMessagesPerVertexStore.map
					.entrySet()) {
				for (Map.Entry<I, ExtendedDataOutput> vertexEntry : partitionEntry
						.getValue().entrySet()) {
					ConcurrentMap<I, ExtendedDataOutput> partitionMap = getOrCreatePartitionMap(partitionEntry
							.getKey());
					ExtendedDataOutput extendedDataOutput = partitionMap
							.get(vertexEntry.getKey());
					if (extendedDataOutput == null) {
						ExtendedDataOutput newExtendedDataOutput = config
								.createExtendedDataOutput();
						extendedDataOutput = partitionMap.putIfAbsent(
								vertexEntry.getKey(), newExtendedDataOutput);
						if (extendedDataOutput == null) {
							extendedDataOutput = newExtendedDataOutput;
						}
					}

					// Add the messages
					extendedDataOutput
							.write(vertexEntry.getValue().getByteArray(), 0,
									vertexEntry.getValue().getPos());
				}
			}
		} else {
			throw new IllegalArgumentException("addMessages: Illegal argument "
					+ messageStore.getClass());
		}
	}

	/**
	 * Factory for {@link IncomeMsgOnlineComputeMessageStore}
	 *
	 * @param <I>
	 *            Vertex id
	 * @param <M>
	 *            Message data
	 */
	private static class Factory<I extends WritableComparable, M extends Writable>
			implements MessageStoreFactory<I, M, MessageStoreByPartition<I, M>> {
		/** Service worker */
		private final CentralizedServiceWorker<I, ?, ?, M> service;
		/** Hadoop configuration */
		private final ImmutableClassesGiraphConfiguration<I, ?, ?, M> config;

		/**
		 * @param service
		 *            Worker service
		 * @param config
		 *            Hadoop configuration
		 */
		public Factory(CentralizedServiceWorker<I, ?, ?, M> service,
				ImmutableClassesGiraphConfiguration<I, ?, ?, M> config) {
			this.service = service;
			this.config = config;
		}

		@Override
		public MessageStoreByPartition<I, M> newStore() {
			return new OnlineComputeMessageStore(service, config);
		}
	}
}
