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
import com.yammer.metrics.core.Counter;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.utils.RepresentativeByteArrayIterable;
import org.apache.giraph.utils.RepresentativeByteArrayIterator;
import org.apache.giraph.utils.VertexIdIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.mortbay.log.Log;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Implementation of {@link SimpleMessageStore} where multiple messages are
 * stored per vertex as byte arrays.  Used when there is no combiner provided.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class ByteArrayMessagesPerVertexStore<I extends WritableComparable,
    M extends Writable> extends SimpleMessageStore<I, M, ExtendedDataOutput> {
  /**
   * Constructor
   *
   * @param service Service worker
   * @param config Hadoop configuration
   */
  public ByteArrayMessagesPerVertexStore(
      CentralizedServiceWorker<I, ?, ?, M> service,
      ImmutableClassesGiraphConfiguration<I, ?, ?, M> config) {
    super(service, config);
  }

  private boolean useSrc2dstEdges = false;
  /**
   * Get the extended data output for a vertex id from the iterator, creating
   * if necessary.  This method will take ownership of the vertex id from the
   * iterator if necessary (if used in the partition map entry).
   *
   * @param partitionMap Partition map to look in
   * @param iterator Special iterator that can release ownerhips of vertex ids
   * @return Extended data output for this vertex id (created if necessary)
   */
  private ExtendedDataOutput getExtendedDataOutput(
      ConcurrentMap<I, ExtendedDataOutput> partitionMap,
      VertexIdIterator<I> iterator) {
    ExtendedDataOutput extendedDataOutput =
        partitionMap.get(iterator.getCurrentVertexId());
    if (extendedDataOutput == null) {
      ExtendedDataOutput newExtendedDataOutput =
          config.createExtendedDataOutput();
      extendedDataOutput =
          partitionMap.putIfAbsent(
              iterator.releaseCurrentVertexId(),
              newExtendedDataOutput);
      if (extendedDataOutput == null) {
        extendedDataOutput = newExtendedDataOutput;
      }
    }
    return extendedDataOutput;
  }

  /**
   * 1. parse srcId of message
   * 2. locate vertexId using outEdgeMap
   * TODO:
   * 3. fill the message with edge value(not supported by now)
   * Get the extended data output for a vertex id from the iterator, creating
   * if necessary.  This method will take ownership of the vertex id from the
   * iterator if necessary (if used in the partition map entry).
   *
   * @param partitionMap Partition map to look in
   * @param iterator Special iterator that can release ownerhips of vertex ids
   * @return Extended data output for this vertex id (created if necessary)
   */
  private void writeIncomeBytesMessage(Map<I, ?> partitionTable, ByteArrayVertexIdMessages<I, M>.VertexIdMessageBytesIterator iterator) {
	I srcId = iterator.getCurrentVertexId();
	List<I> dstVidList = (List<I>) partitionTable.get(srcId);
	if(dstVidList == null)
		return;
	
	for(I dstId:dstVidList){
		//srcId is the id broadcasted from outside, 
		//and dstId is the local vertex id 
		//not possible to fit edgeValue into msgs
//		Object edgeValue = iter.getCurrentSecond();
		int partitionId = this.service.getPartitionId(dstId);
	    ConcurrentMap<I, ExtendedDataOutput> partitionMap =
	            getOrCreatePartitionMap(partitionId);
		ExtendedDataOutput extendedDataOutput =
		        partitionMap.get(dstId);
		if (extendedDataOutput == null) {
	      ExtendedDataOutput newExtendedDataOutput =
	          config.createExtendedDataOutput();
	      extendedDataOutput =
	          partitionMap.putIfAbsent(
	              dstId,
	              newExtendedDataOutput);
	      if (extendedDataOutput == null) {
	          extendedDataOutput = newExtendedDataOutput;
	      }
	    }
		synchronized (extendedDataOutput) {
			iterator.writeCurrentMessageBytes(extendedDataOutput);
		}
	}
  }
  /**
   * 1. parse srcId of message
   * 2. locate vertexId using outEdgeMap
   * 3. fill the message with edge value
   * 4. We better only add reference of the message to the vertex to save space
   * Get the extended data output for a vertex id from the iterator, creating
   * if necessary.  This method will take ownership of the vertex id from the
   * iterator if necessary (if used in the partition map entry).
   *
   * @param partitionMap Partition map to look in
   * @param iterator Special iterator that can release ownerhips of vertex ids
   * @return Extended data output for this vertex id (created if necessary)
   */
  private void writeIncomeMessage(Map<I, ?> partitionTable, ByteArrayVertexIdMessages<I, M>.VertexIdMessageIterator iterator) {
	I srcId = iterator.getCurrentVertexId();
	//release the current message
	M msg = iterator.releaseCurrentData();
	if(useSrc2dstEdges){
		PairList<I, ?> pairList = (PairList<I, ?>) partitionTable.get(srcId);
		if(pairList == null){
	//		Log.info("srcId="+srcId+", no outedge");
			//TODO:add counter to see
			return;
		}
		//pairList not thread-safe!
		synchronized(pairList){
			PairList<I,?>.Iterator iter = pairList.getIterator();
			while(iter.hasNext()){
				iter.next();
				//srcId is the id broadcasted from outside, 
				//and dstId is the local vertex id 
				I dstId = iter.getCurrentFirst();
				Object edgeValue = iter.getCurrentSecond();
				
				int partitionId = this.service.getPartitionId(dstId);
				ConcurrentMap<I, ExtendedDataOutput> partitionMap =
			            getOrCreatePartitionMap(partitionId);
				ExtendedDataOutput extendedDataOutput =
				        partitionMap.get(dstId);
				if (extendedDataOutput == null) {
			      ExtendedDataOutput newExtendedDataOutput =
			          config.createExtendedDataOutput();
			      extendedDataOutput =
			          partitionMap.putIfAbsent(
			              dstId,
			              newExtendedDataOutput);
			      if (extendedDataOutput == null) {
			          extendedDataOutput = newExtendedDataOutput;
			      }
			    }
				if(!(msg instanceof IncomeMessage)){
					//This indicates that edge value is not used for computation 
					synchronized (extendedDataOutput) {
						try {
							msg.write(extendedDataOutput);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
		//			throw new UnsupportedOperationException("Message type is not IncomeMessage! ");
				}else{
					IncomeMessage incomeMsg = (IncomeMessage) msg; 
					synchronized (extendedDataOutput) {
						incomeMsg.setIncomeEdgeValue((Writable)edgeValue);
						try {
							incomeMsg.write(extendedDataOutput);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
	}else{
		List<I> dstIdList = (List<I>) partitionTable.get(srcId);
		if(dstIdList == null){
	//		Log.info("srcId="+srcId+", no outedge");
			//TODO:add counter to see
			return;
		}
		//pairList not thread-safe!
		synchronized(dstIdList){
			for(I dstId:dstIdList){
				//srcId is the id broadcasted from outside, 
				//and dstId is the local vertex id 
				int partitionId = this.service.getPartitionId(dstId);
				ConcurrentMap<I, ExtendedDataOutput> partitionMap =
			            getOrCreatePartitionMap(partitionId);
				ExtendedDataOutput extendedDataOutput =
				        partitionMap.get(dstId);
				if (extendedDataOutput == null) {
			      ExtendedDataOutput newExtendedDataOutput =
			          config.createExtendedDataOutput();
			      extendedDataOutput =
			          partitionMap.putIfAbsent(
			              dstId,
			              newExtendedDataOutput);
			      if (extendedDataOutput == null) {
			          extendedDataOutput = newExtendedDataOutput;
			      }
			    }
				if(!(msg instanceof IncomeMessage)){
					//This indicates that edge value is not used for computation 
					synchronized (extendedDataOutput) {
						try {
							msg.write(extendedDataOutput);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
		//			throw new UnsupportedOperationException("Message type is not IncomeMessage! ");
				}else{
					throw new UnsupportedOperationException("IncomeMessage is not supported when using srcvid2dstvids! ");
				}
			}
		}
	}
  }
  @Override
  public void addPartitionMessages(
      int partitionId,
      ByteArrayVertexIdMessages<I, M> messages) throws IOException {
    ConcurrentMap<I, ExtendedDataOutput> partitionMap =
        getOrCreatePartitionMap(partitionId);
    ByteArrayVertexIdMessages<I, M>.VertexIdMessageBytesIterator
        vertexIdMessageBytesIterator =
        messages.getVertexIdMessageBytesIterator();
    // Try to copy the message buffer over rather than
    // doing a deserialization of a message just to know its size.  This
    // should be more efficient for complex objects where serialization is
    // expensive.  If this type of iterator is not available, fall back to
    // deserializing/serializing the messages
    if (vertexIdMessageBytesIterator != null) {
      while (vertexIdMessageBytesIterator.hasNext()) {
        vertexIdMessageBytesIterator.next();
        ExtendedDataOutput extendedDataOutput =
            getExtendedDataOutput(partitionMap, vertexIdMessageBytesIterator);

        synchronized (extendedDataOutput) {
          vertexIdMessageBytesIterator.writeCurrentMessageBytes(
              extendedDataOutput);
        }
      }
    } else {
      ByteArrayVertexIdMessages<I, M>.VertexIdMessageIterator
          vertexIdMessageIterator = messages.getVertexIdMessageIterator();
      while (vertexIdMessageIterator.hasNext()) {
        vertexIdMessageIterator.next();
        ExtendedDataOutput extendedDataOutput =
            getExtendedDataOutput(partitionMap, vertexIdMessageIterator);

        synchronized (extendedDataOutput) {
          vertexIdMessageIterator.getCurrentMessage().write(
              extendedDataOutput);
        }
      }
    }
  }

  @Override
  public void addIncomePartitionMessages(
      int partitionId,
      ByteArrayVertexIdMessages<I, M> messages) throws IOException {
	  
	  //TODO: direct byte copy not supported by now
	    ByteArrayVertexIdMessages<I, M>.VertexIdMessageBytesIterator
	        vertexIdMessageBytesIterator =
	        messages.getVertexIdMessageBytesIterator();
	    Map<I, ?> partitionTable = null;
	      if(useSrc2dstEdges)
	    	  partitionTable = service.getServerData().getEdgeStore().getOutEdgePartition(partitionId);
	      else
	    	  partitionTable = service.getServerData().getEdgeStore().getDstVidPartition(partitionId); 
	    // Try to copy the message buffer over rather than
	    // doing a deserialization of a message just to know its size.  This
	    // should be more efficient for complex objects where serialization is
	    // expensive.  If this type of iterator is not available, fall back to
	    // deserializing/serializing the messages
	    if (vertexIdMessageBytesIterator != null) {
	      while (vertexIdMessageBytesIterator.hasNext()) {
	        vertexIdMessageBytesIterator.next();
	        writeIncomeBytesMessage(partitionTable, vertexIdMessageBytesIterator);
	      }
	    } else {
	      ByteArrayVertexIdMessages<I, M>.VertexIdMessageIterator
	      vertexIdMessageIterator = messages.getVertexIdMessageIterator();
	     
	      while (vertexIdMessageIterator.hasNext()) {
	        vertexIdMessageIterator.next();
	        writeIncomeMessage(partitionTable, vertexIdMessageIterator);
	      }
	    }
  }
  /**
   * Special iterable that recycles the message
   */
  private class MessagesIterable extends RepresentativeByteArrayIterable<M> {
    /**
     * Constructor
     *
     * @param buf Buffer
     * @param off Offset to start in the buffer
     * @param length Length of the buffer
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
     * @param configuration Configuration
     * @param buf buffer to read from
     * @param off Offset into the buffer to start from
     * @param length Length of the buffer
     */
    public RepresentativeMessageIterator(
        ImmutableClassesGiraphConfiguration configuration,
        byte[] buf, int off, int length) {
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
      numberOfMessages += Iterators.size(
          new RepresentativeMessageIterator(config,
              extendedDataOutput.getByteArray(), 0,
              extendedDataOutput.getPos()));
    }
    return numberOfMessages;
  }

  @Override
  protected void writeMessages(ExtendedDataOutput extendedDataOutput,
      DataOutput out) throws IOException {
    out.writeInt(extendedDataOutput.getPos());
    out.write(
        extendedDataOutput.getByteArray(), 0, extendedDataOutput.getPos());
  }

  @Override
  protected ExtendedDataOutput readFieldsForMessages(DataInput in) throws
      IOException {
    int byteArraySize = in.readInt();
    byte[] messages = new byte[byteArraySize];
    in.readFully(messages);
    ExtendedDataOutput extendedDataOutput =
        config.createExtendedDataOutput(messages, 0);
    return extendedDataOutput;
  }

  /**
   * Create new factory for this message store
   *
   * @param service Worker service
   * @param config  Hadoop configuration
   * @param <I>     Vertex id
   * @param <M>     Message data
   * @return Factory
   */
  public static <I extends WritableComparable, M extends Writable>
  MessageStoreFactory<I, M, MessageStoreByPartition<I, M>> newFactory(
      CentralizedServiceWorker<I, ?, ?, M> service,
      ImmutableClassesGiraphConfiguration<I, ?, ?, M> config) {
    return new Factory<I, M>(service, config);
  }

  @Override
  public void addMessages(MessageStore<I, M> messageStore) throws IOException {
    if (messageStore instanceof ByteArrayMessagesPerVertexStore) {
      ByteArrayMessagesPerVertexStore<I, M>
          byteArrayMessagesPerVertexStore =
          (ByteArrayMessagesPerVertexStore<I, M>) messageStore;
      for (Map.Entry<Integer, ConcurrentMap<I, ExtendedDataOutput>>
           partitionEntry : byteArrayMessagesPerVertexStore.map.entrySet()) {
        for (Map.Entry<I, ExtendedDataOutput> vertexEntry :
            partitionEntry.getValue().entrySet()) {
          ConcurrentMap<I, ExtendedDataOutput> partitionMap =
              getOrCreatePartitionMap(partitionEntry.getKey());
          ExtendedDataOutput extendedDataOutput =
              partitionMap.get(vertexEntry.getKey());
          if (extendedDataOutput == null) {
            ExtendedDataOutput newExtendedDataOutput =
                config.createExtendedDataOutput();
            extendedDataOutput =
                partitionMap.putIfAbsent(vertexEntry.getKey(),
                    newExtendedDataOutput);
            if (extendedDataOutput == null) {
              extendedDataOutput = newExtendedDataOutput;
            }
          }

          // Add the messages
          extendedDataOutput.write(vertexEntry.getValue().getByteArray(), 0,
              vertexEntry.getValue().getPos());
        }
      }
    } else {
      throw new IllegalArgumentException("addMessages: Illegal argument " +
          messageStore.getClass());
    }
  }

  /**
   * Factory for {@link ByteArrayMessagesPerVertexStore}
   *
   * @param <I> Vertex id
   * @param <M> Message data
   */
  private static class Factory<I extends WritableComparable, M extends Writable>
      implements MessageStoreFactory<I, M, MessageStoreByPartition<I, M>> {
    /** Service worker */
    private final CentralizedServiceWorker<I, ?, ?, M> service;
    /** Hadoop configuration */
    private final ImmutableClassesGiraphConfiguration<I, ?, ?, M> config;

    /**
     * @param service Worker service
     * @param config  Hadoop configuration
     */
    public Factory(CentralizedServiceWorker<I, ?, ?, M> service,
        ImmutableClassesGiraphConfiguration<I, ?, ?, M> config) {
      this.service = service;
      this.config = config;
    }

    @Override
    public MessageStoreByPartition<I, M> newStore() {
      return new ByteArrayMessagesPerVertexStore(service, config);
    }
  }
}
