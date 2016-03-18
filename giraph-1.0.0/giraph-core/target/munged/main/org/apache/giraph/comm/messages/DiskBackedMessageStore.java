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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.RepresentativeByteArrayIterable;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Message storage with in-memory map of messages and with support for
 * flushing all the messages to the disk.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class DiskBackedMessageStore<I extends WritableComparable,
    M extends Writable> implements FlushableMessageStore<I, M> {
  public int partitionId;
  private static final Logger LOG = Logger.getLogger(DiskBackedMessageStore.class);

  /** in-mem msgStore. no order is needed! pay attention that <I,M> should write as a pair! **/
  private volatile ExtendedDataOutput extendedDataOutput;
  /** File stores in which we keep flushed messages */
  private SequentialFileMessageStore<I, M> fileStore;
  MessageStoreFactory<I, M, BasicMessageStore<I, M>> fileStoreFactory;
  /** Hadoop configuration */
  private final ImmutableClassesGiraphConfiguration<I, ?, ?, M> config;
  /** Counter for number of messages in-memory */
  private final AtomicInteger numberOfMessagesInMemory;
  /** Factory for creating file stores when flushing */
  /** Lock for disk flushing */
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

  /**
   * Constructor.
   *
   * @param config           Hadoop configuration
   * @param fileStoreFactory Factory for creating file stores when flushing
   */
  public DiskBackedMessageStore(
      ImmutableClassesGiraphConfiguration<I, ?, ?, M> config,
      MessageStoreFactory<I, M, BasicMessageStore<I, M>> fileStoreFactory) {
    this.config = config;
    extendedDataOutput = config.createExtendedDataOutput();
    numberOfMessagesInMemory = new AtomicInteger(0);
//    fileStores = Lists.newArrayList();
    fileStore = (SequentialFileMessageStore<I, M>) fileStoreFactory.newStore();
    this.fileStoreFactory = fileStoreFactory;
  }

  /**
   * Add vertex messages
   *
   * @param vertexId Vertex id to use
   * @param messages Messages to add (note that the lifetime of the messages)
   *                 is only until next() is called again)
   * @return True if the vertex id ownership is taken by this method,
   *         false otherwise
   * @throws IOException
   */
  boolean addVertexMessages(I vertexId,
                            Iterable<M> messages) throws IOException {
    boolean ownsVertexId = false;
    rwLock.readLock().lock();
    try {
      ownsVertexId = false;
      synchronized (extendedDataOutput) {
        for (M message : messages) {
          vertexId.write(extendedDataOutput);	
          message.write(extendedDataOutput);
          numberOfMessagesInMemory.getAndIncrement();
        }
      }
    } finally {
      rwLock.readLock().unlock();
    }

    return ownsVertexId;
  }

  @Override
  public void addMessages(MessageStore<I, M> messageStore) throws
      IOException {
    for (I destinationVertex : messageStore.getDestinationVertices()) {
      addVertexMessages(destinationVertex,
          messageStore.getVertexMessages(destinationVertex));
    }
  }

  /**
   * Special iterable that recycles the message
   */
  private class MessageIterable extends RepresentativeByteArrayIterable<M> {
    /**
     * Constructor
     *
     * @param buf Buffer
     * @param off Offset to start in the buffer
     * @param length Length of the buffer
     */
    public MessageIterable(
        byte[] buf, int off, int length) {
      super(config, buf, off, length);
    }

    @Override
    protected M createWritable() {
      return config.createMessageValue();
    }
  }
  

//  public class IMIterator{
//	  ImmutableClassesGiraphConfiguration<I, ?, ?, M> conf;
//	  I currentVertexId;
//	  M currentMessage;
//	  ExtendedDataInput inmemDataBuffer;
//	  boolean hasNext;
//	  boolean inmemFinish;
//	  SequentialFileMessageStore<I, M> msgStore;
//	  public IMIterator(ImmutableClassesGiraphConfiguration<I, ?, ?, M> conf,
//			  ExtendedDataOutput inmemDataOutputBuffer,
//			  SequentialFileMessageStore<I, M> msgStore){
//		  this.conf = conf;
//		  
//		  this.inmemDataBuffer =
//			        conf.createExtendedDataInput(inmemDataOutputBuffer.getByteArray(),
//			        		0, inmemDataOutputBuffer.getPos());
//		  this.msgStore = msgStore;
//		  currentVertexId = conf.createVertexId();
//		  currentMessage = conf.createMessageValue();
//		  if(inmemDataBuffer.available()>0)
//			  inmemFinish = false;
//	  }
//	  public boolean hasNext(){
//		  if(!inmemFinish)
//			  return inmemDataBuffer.available()>0;
//		  else{
//			  return msgStore.getVertexMessages(vertexId)
//		  }
//	  }
//	  public void next(){};
//	  public M getCurrentMessage(){
//		  
//	  }
//	  public I getCurrentVertexId(){
//		  
//	  }
//  }
  public Iterable<IMWritable<I,M>> getPartitionMessages(){
	 Iterable<IMWritable<I,M>> combinedIterable = new IMIterable<I,M>(
		        extendedDataOutput.getByteArray(), 0, extendedDataOutput.getPos(), config);
	 try {
		combinedIterable = Iterables.concat(combinedIterable,
		          fileStore.getPartitionMessagesOnDisk());
	} catch (IOException e) {
		e.printStackTrace();
	}
	 return combinedIterable;
  }
  
  @Override
  public Iterable<M> getVertexMessages(I vertexId) throws IOException {
//    ExtendedDataOutput extendedDataOutput = inMemoryMessages.get(vertexId);
//    if (extendedDataOutput == null) {
//      extendedDataOutput = config.createExtendedDataOutput();
//    }
//    Iterable<M> combinedIterable = new MessageIterable(
//        extendedDataOutput.getByteArray(), 0, extendedDataOutput.getPos());
//
//    for (BasicMessageStore<I, M> fileStore : fileStores) {
//      combinedIterable = Iterables.concat(combinedIterable,
//          fileStore.getVertexMessages(vertexId));
//    }
//    return combinedIterable;
	  return null;
  }

  @Override
  public int getNumberOfMessages() {
    return numberOfMessagesInMemory.get();
  }

  @Override
  public boolean hasMessagesForVertex(I vertexId) {
//    return destinationVertices.contains(vertexId);
	  return false;
  }

  @Override
  public Iterable<I> getDestinationVertices() {
//    return destinationVertices;
	  return Collections.emptyList();
  }

  @Override
  public void clearVertexMessages(I vertexId) throws IOException {
//    inMemoryMessages.remove(vertexId);
  }

  @Override
  public void clearAll() throws IOException {
//    inMemoryMessages.clear();
//    destinationVertices.clear();
//    for (BasicMessageStore<I, M> fileStore : fileStores) {
//      fileStore.clearAll();
//    }
//    fileStores.clear();
	  fileStore.clearAll();
  }

  /**
   * Special temporary message store for passing along in-memory messages
   */
  public class TemporaryMessageStore implements MessageStore<I, M> {
//    /**
//     * In-memory message map (must be sorted to insure that the ids are
//     * ordered)
//     */
//    private final ConcurrentNavigableMap<I, ExtendedDataOutput>
//    temporaryMessages;
    ExtendedDataOutput extendedDataOuptut;
    /**
     * Constructor.
     *
     * @param temporaryMessages Messages to be owned by this object
     */
    public TemporaryMessageStore(
        ExtendedDataOutput extendedDataOuptut) {
      this.extendedDataOuptut = extendedDataOuptut;
    }

    @Override
    public int getNumberOfMessages() {
      throw new IllegalAccessError("getNumberOfMessages: Not supported");
    }

    @Override
    public boolean hasMessagesForVertex(I vertexId) {
    	 throw new IllegalAccessError("write: Not supported");
    }

    @Override
    public Iterable<I> getDestinationVertices() {
    	 throw new IllegalAccessError("write: Not supported");
    }

    @Override
    public void addMessages(MessageStore<I, M> messageStore)
      throws IOException {
      throw new IllegalAccessError("addMessages: Not supported");
    }

    @Override
    public Iterable<M> getVertexMessages(I vertexId) throws IOException {
    	 throw new IllegalAccessError("write: Not supported");
    }

    @Override
    public void clearVertexMessages(I vertexId) throws IOException {
    	 throw new IllegalAccessError("clearVmsg: Not supported");
    }

    @Override
    public void clearAll() throws IOException {
    	extendedDataOuptut = null;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      throw new IllegalAccessError("write: Not supported");
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      throw new IllegalAccessError("readFields: Not supported");
    }

  }

  public int getInMemMsgSize(){
	  return extendedDataOutput.getPos();
  }
  
  @Override
  public void flush() throws IOException {
	  ExtendedDataOutput extendedDataOutputToFlush = null;
    rwLock.writeLock().lock();
    try {
    	extendedDataOutputToFlush = extendedDataOutput;
    	extendedDataOutput = config.createExtendedDataOutput();
      numberOfMessagesInMemory.set(0);
    } finally {
      rwLock.writeLock().unlock();
    }
//    BasicMessageStore<I, M> fileStore = fileStoreFactory.newStore();
//    LOG.info("add msg to Disk For partitionID="+partitionId);
    fileStore.addMessages(new TemporaryMessageStore(extendedDataOutputToFlush));
  }

  @Override
  public void write(DataOutput out) throws IOException {
	  // write in-memory messages 
	  out.writeInt(numberOfMessagesInMemory.get());
	  out.writeInt(extendedDataOutput.getByteArray().length);
	  out.write(extendedDataOutput.getByteArray());
	  
    // write file stores
    fileStore.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // read in-memory messages
    numberOfMessagesInMemory.set(in.readInt());
    int bytelen = in.readInt();
    this.extendedDataOutput = config.createExtendedDataOutput(bytelen);
    in.readFully(extendedDataOutput.getByteArray(), 0, bytelen);
    fileStore = (SequentialFileMessageStore<I, M>) fileStoreFactory.newStore();
  }


  /**
   * Create new factory for this message store
   *
   * @param config           Hadoop configuration
   * @param fileStoreFactory Factory for creating message stores for
   *                         partitions
   * @param <I>              Vertex id
   * @param <M>              Message data
   * @return Factory
   */
  public static <I extends WritableComparable, M extends Writable>
  MessageStoreFactory<I, M, FlushableMessageStore<I, M>> newFactory(
      ImmutableClassesGiraphConfiguration<I, ?, ?, M> config,
      MessageStoreFactory<I, M, BasicMessageStore<I, M>> fileStoreFactory) {
    return new Factory<I, M>(config, fileStoreFactory);
  }

  /**
   * Factory for {@link DiskBackedMessageStore}
   *
   * @param <I> Vertex id
   * @param <M> Message data
   */
  private static class Factory<I extends WritableComparable,
      M extends Writable> implements MessageStoreFactory<I, M,
      FlushableMessageStore<I, M>> {
    /** Hadoop configuration */
    private final ImmutableClassesGiraphConfiguration config;
    /** Factory for creating message stores for partitions */
    private final
    MessageStoreFactory<I, M, BasicMessageStore<I, M>> fileStoreFactory;

    /**
     * @param config           Hadoop configuration
     * @param fileStoreFactory Factory for creating message stores for
     *                         partitions
     */
    public Factory(ImmutableClassesGiraphConfiguration config,
        MessageStoreFactory<I, M, BasicMessageStore<I, M>> fileStoreFactory) {
      this.config = config;
      this.fileStoreFactory = fileStoreFactory;
    }

    @Override
    public FlushableMessageStore<I, M> newStore() {
      return new DiskBackedMessageStore<I, M>(config, fileStoreFactory);
    }
  }

	@Override
	public int getPartitionId() {
		return partitionId;
	}

	@Override
	public void setPartitionId(int partitionId) {
		// TODO Auto-generated method stub
		this.partitionId = partitionId;
	}
}
