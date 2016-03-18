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

package org.apache.giraph.partition;

import static org.apache.giraph.conf.GiraphConstants.MAX_PARTITIONS_IN_MEMORY;
import static org.apache.giraph.conf.GiraphConstants.PARTITIONS_DIRECTORY;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.collections.MapUtils;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Disk-backed PartitionStore. Partitions are stored in memory on a LRU basis.
 * Thread-safe, but expects the caller to synchronized between deletes, adds,
 * puts and gets.
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
@SuppressWarnings("rawtypes")
public class DiskBackedOnlineComputePartitionStore<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable>
		extends PartitionStore<I, V, E, M> {
	/** Class logger. */
	private static final Logger LOG = Logger
			.getLogger(DiskBackedOnlineComputePartitionStore.class);

	/** States the partition can be found in */
	public enum State {
		ACTIVE, INACTIVE, LOADING, OFFLOADING, ONDISK
	};

	/** Global lock to the whole partition */
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	/**
	 * Global write lock. Must be hold to modify class state for read and write.
	 * Conditions are bond to this lock.
	 */
	private final Lock wLock = lock.writeLock();
	// private final Lock rLock = lock.readLock();
	/** The ids of the partitions contained in the store */
	private final Set<Integer> partitionIds = Sets.newHashSet();
	/** Record the hot value of each partition */
	private Map<Integer, Long> hotMap = Maps.newHashMap();
	/**
	 * sortedPartitionOnHot[i]=partitionId means the i-th hottest partition's id
	 * is partitionId
	 */
	private ArrayList<Integer> sortedPartitionOnHot;
	/** Partitions' states store */
	private final Map<Integer, State> states = Maps.newHashMap();
	/** Current active partitions, which have not been put back yet */
	private final Map<Integer, Partition<I, V, E, M>> active = Maps
			.newHashMap();
	/** Inactive partitions to re-activate or spill to disk to make space */
	private final Map<Integer, Partition<I, V, E, M>> inactive = Maps
			.newLinkedHashMap();
	private Map<Integer, Partition<I, V, E, M>> inactiveColdPartitions;

	/** Ids of partitions stored on disk and number of vertices contained */
	private final Map<Integer, Integer> onDiskVertexCountMap = Maps
			.newHashMap();
	private final Map<Integer, Long> edgeCountMap = Maps.newHashMap();
	private final Map<Integer, Long> vertexCountMap = Maps.newHashMap();
	/** Per-partition users counters (clearly only for active partitions) */
	private final Map<Integer, Integer> counters = Maps.newHashMap();
	/** These Conditions are used to partitions' change of state */
	private final Map<Integer, Condition> pending = Maps.newHashMap();
	/**
	 * Used to signal threads waiting to load partitions. Can be used when new
	 * inactive partitions are avaiable, or when free slots are available.
	 */
	private final Condition notEmpty = wLock.newCondition();
	/** Executors for users requests. Uses caller threads */
	private final ExecutorService pool = new DirectExecutorService();
	/** Giraph configuration */
	private final ImmutableClassesGiraphConfiguration<I, V, E, M> conf;
	/** Mapper context */
	private final Context context;
	/** Base path where the partition files are written to */
	private final String[] basePaths;
	/** Used to hash partition Ids */
	private final HashFunction hasher = Hashing.murmur3_32();
	/** Maximum number of slots */
	private final int maxInMemoryPartitions;
	/** Number of slots used */
	private int inMemoryPartitions;

	/** whether we need to move edges out to let more vertex in */
	private boolean edgeSeparation = false;

	private boolean isStaticGraph = true;
	/**
	 * Constructor
	 * 
	 * @param conf
	 *            Configuration
	 * @param context
	 *            Context
	 */
	public DiskBackedOnlineComputePartitionStore(
			ImmutableClassesGiraphConfiguration<I, V, E, M> conf,
			Mapper<?, ?, ?, ?>.Context context) {
		this.conf = conf;
		this.context = context;
		// We must be able to hold at least one partition in memory
		maxInMemoryPartitions = Math.max(MAX_PARTITIONS_IN_MEMORY.get(conf), 1);
		edgeSeparation = conf.useEdgeSeparation();
		// Take advantage of multiple disks
		String[] userPaths = PARTITIONS_DIRECTORY.getArray(conf);
		basePaths = new String[userPaths.length];
		int i = 0;
		for (String path : userPaths) {
			basePaths[i++] = path + "/"
					+ conf.get("mapred.job.id", "Unknown Job");
		}
	}

	/**
	 * copy the current onlineComputedValue to value. (hot partition only)
	 */
//	public void finishSuperstep() {
//		LOG.info("copy onlineComputeValue to value begin, only for hot partition "
//				+ "because they reside in memory permanently");
//		long s = System.currentTimeMillis();
//		for (Integer id : hotPartitionIds) {
//			Partition<I, V, E, M> p = getPartition(id);
//			for (Vertex<I, V, E, M> vertex : p) {
//				vertex.finishSuperstep();
//			}
//		}
//		long t = System.currentTimeMillis();
//		LOG.info("use " + (t - s) * 1.0 / 1000 + " s");
//	}

	public void hotAwareRepartition(int newPartitionNum) {
		for (Integer i : inactive.keySet()) {
			Partition p = getPartition(i);
		}
	}

	/**
	 * Set the hot value of each partition, must be called after the
	 * partitionStore is fully constructed. Notice that the following graph
	 * updates are not reflected in the partition hot value
	 */
	public void hotAnalysis() {
		LOG.info("old hot analysis");
		hotMap = Maps.newHashMap(edgeCountMap);
		sortedPartitionOnHot = new ArrayList<Integer>(edgeCountMap.keySet());
		Collections.sort(sortedPartitionOnHot, new Comparator<Integer>() {

			@Override
			public int compare(Integer o1, Integer o2) {
				Long hot1 = hotMap.get(o1);
				Long hot2 = hotMap.get(o2);
				if (hot1 > hot2)
					return -1;
				else if (hot1 == hot2)
					return 0;
				else
					return 1;
			}

		});
		// hot partition are all reside in memory and do not swap to disk.
		int hotPartitionNum = maxInMemoryPartitions - 1;
		hotPartitionIds = new HashSet<Integer>();

		for (int i = 0; i < hotPartitionNum; i++)
			hotPartitionIds.add(sortedPartitionOnHot.get(i));

		// copy inactive partitions to inactiveColdPartitions if it's cold.
		inactiveColdPartitions = Maps.newLinkedHashMap();
		for (Entry<Integer, Partition<I, V, E, M>> entry : inactive.entrySet()) {
			if (!hotPartitionIds.contains(entry.getKey())) {
				inactiveColdPartitions.put(entry.getKey(), entry.getValue());
			}
		}
		// cold partition would be swap to disk when memory is not enough.
		int coldPartitionNum = partitionIds.size() - hotPartitionNum;
		StringBuffer hotP = new StringBuffer();
		for (Integer id : hotPartitionIds) {
			long hotValue = this.hotMap.get(id);
			hotP.append("(" + id + ", " + hotValue + ")|");
		}
		LOG.info("HOTS analysis: HOT partition HOTS:" + hotP.toString());
		hotP = new StringBuffer();
		for (Integer id : sortedPartitionOnHot) {
			hotP.append("(" + id + ", " + hotMap.get(id) + ")|");
		}
		LOG.info("HOTS analysis: ALL partition HOTS:" + hotP.toString());
		LOG.info("now adjust memory to fill with hot partitions...");
		for (Integer id : hotPartitionIds) {
			Partition<I, V, E, M> p = getPartitionIfInMemory(id);
			if (p == null)
				p = getPartition(id);
			putPartition(p, 1);
		}
		for (Entry<Integer, Partition<I, V, E, M>> entry : inactiveColdPartitions
				.entrySet()) {
			inMemColdPartitionId = entry.getKey();
		}
	}

	Set<Integer> hotPartitionIds;
	HashMap<Writable, Integer> lookUp = Maps.newHashMap();

	public void hotRepartition() {
		hotPartitionIds = new HashSet<Integer>();
		inactiveColdPartitions = Maps.newLinkedHashMap();
		// hot partition are all reside in memory and do not swap to disk.
		int pNum = edgeCountMap.size();
		int hotPartitionNum = maxInMemoryPartitions - 1;
		Partition<I, V, E, M>[] partitions = new Partition[pNum];
		DataOutputStream[] node_outputStream = new DataOutputStream[pNum];
		DataOutputStream[] edge_outputStream = new DataOutputStream[pNum];
		int[] edgeCount = new int[pNum];
		int[] nodeCount = new int[pNum];
		try {
			for (int i = 0; i <= hotPartitionNum; i++) {
				
				partitions[i] = conf.createPartition(i, context);
				if(edgeSeparation){
					File file1 = new File(getRePartitionPath(i) + "_edges");
					file1.getParentFile().mkdirs();
					file1.createNewFile();
					edge_outputStream[i] = new DataOutputStream(
							new BufferedOutputStream(new FileOutputStream(file1,
									true)));
				}
			}
			for (int i = hotPartitionNum + 1; i < pNum; i++) {
				File file = new File(getRePartitionPath(i) + "_vertices");
				file.getParentFile().mkdirs();
				file.createNewFile();
				node_outputStream[i] = new DataOutputStream(
						new BufferedOutputStream(new FileOutputStream(file,
								true)));

				File file1 = new File(getRePartitionPath(i) + "_edges");
				file1.getParentFile().mkdirs();
				file1.createNewFile();
				edge_outputStream[i] = new DataOutputStream(
						new BufferedOutputStream(new FileOutputStream(file1,
								true)));
			}
			int t=conf.getInt("giraph.hotPartitionSortBufferMultiplier", 100);
			int bNum = pNum*t;
			Vertex<I, V, E, M>[] buffer = new Vertex[bNum];
			int cur = 0;
			Set<Integer> processed = new HashSet<Integer>(inactive
					.keySet());
			for (Integer pId : processed) {
				Partition<I, V, E, M> p = getPartition(pId, 1);
				active.remove(pId);
				for (Vertex<I, V, E, M> v : p) {
					buffer[cur++] = v;
					if (cur == bNum) {
						// sort
						Arrays.sort(buffer, new Comparator<Vertex>() {

							@Override
							public int compare(Vertex o1, Vertex o2) {
								int n1 = o1.getNumEdges();
								int n2 = o2.getNumEdges();
								if (n1 > n2)
									return -1;
								else if (n1 == n2)
									return 0;
								else
									return 1;
							}

						});
						int i = 0;
						// assign
						for (int k = 0; k < bNum; ) {
							lookUp.put(buffer[k].getId(), i);
							nodeCount[i]++;
							edgeCount[i] += buffer[k].getNumEdges();
							if (i <= hotPartitionNum){
								partitions[i].putVertex(buffer[k]);
								if(edgeSeparation){
									writeOutEdges(edge_outputStream[i], buffer[k]);
									buffer[k].setEdges(null);
								}
							}
							else {
								writeVertexData(node_outputStream[i], buffer[k]);
								writeOutEdges(edge_outputStream[i], buffer[k]);
							}
							if((++k)%t==0)
								i++;
						}
						cur = 0;
					}
				}
			}
			inMemoryPartitions = 0;
			for (Integer key : edgeCountMap.keySet()) {
				if (processed.contains(key))
					continue;
				int pId = key;
				Partition<I, V, E, M> p = getPartition(pId, 1);
				for (Vertex<I, V, E, M> v : p) {
					buffer[cur++] = v;
					if (cur == bNum) {
						// sort
						Arrays.sort(buffer, new Comparator<Vertex>() {

							@Override
							public int compare(Vertex o1, Vertex o2) {
								int n1 = o1.getNumEdges();
								int n2 = o2.getNumEdges();
								if (n1 > n2)
									return -1;
								else if (n1 == n2)
									return 0;
								else
									return 1;
							}

						});
						
						int i = 0;
						// assign
						for (int k = 0; k < bNum;) {
							lookUp.put(buffer[k].getId(), i);
							nodeCount[i]++;
							edgeCount[i] += buffer[k].getNumEdges();
							
							if (i <= hotPartitionNum){
								partitions[i].putVertex(buffer[k]);
								if(edgeSeparation){
									writeOutEdges(edge_outputStream[i], buffer[k]);
									buffer[k].setEdges(null);
								}
							}
							else {
								writeVertexData(node_outputStream[i], buffer[k]);
								writeOutEdges(edge_outputStream[i], buffer[k]);
							}
							
							if((++k)%t==0)
								i++;
						}
						cur = 0;
						
					}
				}
				active.remove(pId);
				inMemoryPartitions--;
			}
			repartitioned = true;
			int i = 0;
			// assign
			for (int k = 0; k < cur;) {
				lookUp.put(buffer[k].getId(), i);
				nodeCount[i]++;
				edgeCount[i] += buffer[k].getNumEdges();
				if (i <= hotPartitionNum){
					partitions[i].putVertex(buffer[k]);
					if(edgeSeparation){
						writeOutEdges(edge_outputStream[i], buffer[k]);
						buffer[k].setEdges(null);
					}
				}
				else {
					writeVertexData(node_outputStream[i], buffer[k]);
					writeOutEdges(edge_outputStream[i], buffer[k]);
				}
				
				if((++k)%t==0)
					i++;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for (int i = 0; i < pNum; i++) {
			try {
				if(node_outputStream[i]!=null)
					node_outputStream[i].close();
				if(edge_outputStream[i]!=null)
					edge_outputStream[i].close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		// adjust control structures
		states.clear();
		partitionIds.clear();
		hotPartitionIds.clear();
		active.clear();
		inactive.clear();
		edgeCountMap.clear();
		vertexCountMap.clear();
		onDiskVertexCountMap.clear();
		counters.clear();
		pending.clear();
		for (int i = 0; i < pNum; i++) {
			Condition newC = wLock.newCondition();
			pending.put(i, newC);
		}
		inMemoryPartitions = hotPartitionNum+1;

		for (int i = 0; i < pNum; i++) {
			partitionIds.add(i);
			counters.put(i, 0);
			vertexCountMap.put(i, (long) nodeCount[i]);
			edgeCountMap.put(i, (long) edgeCount[i]);
			if (i <= hotPartitionNum) {
				states.put(i, State.INACTIVE);
				inactive.put(i, partitions[i]);
				if (i == hotPartitionNum) {
					inactiveColdPartitions.put(i, partitions[i]);
					inMemColdPartitionId = i;
				} else
					hotPartitionIds.add(i);
			} else {
				onDiskVertexCountMap.put(i, nodeCount[i]);
				states.put(i, State.ONDISK);
			}
		}
		StringBuffer edgeStatus = new StringBuffer();
		for (Entry<Integer, Long> entry : edgeCountMap.entrySet()) {
			edgeStatus.append(entry.getValue() + "\t");
		}
		LOG.info("HOTS analysis: repartition results:edgeCountMap:"
				+ edgeStatus.toString());
	}

	@Override
	public Iterable<Integer> getPartitionIds() {
		try {
			return pool.submit(new Callable<Iterable<Integer>>() {

				@Override
				public Iterable<Integer> call() throws Exception {
					wLock.lock();
					try {
						return Iterables.unmodifiableIterable(partitionIds);
					} finally {
						wLock.unlock();
					}
				}
			}).get();
		} catch (InterruptedException e) {
			throw new IllegalStateException(
					"getPartitionIds: cannot retrieve partition ids", e);
		} catch (ExecutionException e) {
			throw new IllegalStateException(
					"getPartitionIds: cannot retrieve partition ids", e);
		}
	}

	@Override
	public boolean hasPartition(final Integer id) {
		try {
			return pool.submit(new Callable<Boolean>() {

				@Override
				public Boolean call() throws Exception {
					wLock.lock();
					try {
						return partitionIds.contains(id);
					} finally {
						wLock.unlock();
					}
				}
			}).get();
		} catch (InterruptedException e) {
			throw new IllegalStateException(
					"hasPartition: cannot check partition", e);
		} catch (ExecutionException e) {
			throw new IllegalStateException(
					"hasPartition: cannot check partition", e);
		}
	}

	@Override
	public int getNumPartitions() {
		try {
			return pool.submit(new Callable<Integer>() {

				@Override
				public Integer call() throws Exception {
					wLock.lock();
					try {
						return partitionIds.size();
					} finally {
						wLock.unlock();
					}
				}
			}).get();
		} catch (InterruptedException e) {
			throw new IllegalStateException(
					"getNumPartitions: cannot retrieve partition ids", e);
		} catch (ExecutionException e) {
			throw new IllegalStateException(
					"getNumPartitions: cannot retrieve partition ids", e);
		}
	}

	/**
	 * callType==0:normal get callType==1: only works if vertexOnly is true(hot
	 * edges are on disk) if partition is in memory, load edges if partition is
	 * on disk, load partition and the associated edges callType==2: only works
	 * if vertexOnly is true(hot edges are on disk) and only for hot analysis if
	 * partition is in memory, do not load edges, if partition is on disk, load
	 * partition only with vertices
	 * 
	 * @param id
	 * @param callType
	 * @return
	 */
	public Partition<I, V, E, M> getPartition(Integer id, int callType) {
		try {
			if (!hotPartitionIds.contains(id))
				inMemColdPartitionId = id;
			GetPartition gp = new GetPartition(id);
			gp.setCallType(callType);
			return pool.submit(gp).get();
		} catch (InterruptedException e) {
			throw new IllegalStateException(
					"getPartition: cannot retrieve partition " + id, e);
		} catch (ExecutionException e) {
			throw new IllegalStateException(
					"getPartition: cannot retrieve partition " + id, e);
		}
	}

	int inMemColdPartitionId = -1;

	public int getInMemColdParititionId() {
		return inMemColdPartitionId;
	}

	@Override
	public Partition<I, V, E, M> getPartition(Integer id) {
		try {
			if (!hotPartitionIds.contains(id))
				inMemColdPartitionId = id;
			return pool.submit(new GetPartition(id)).get();
		} catch (InterruptedException e) {
			throw new IllegalStateException(
					"getPartition: cannot retrieve partition " + id, e);
		} catch (ExecutionException e) {
			throw new IllegalStateException(
					"getPartition: cannot retrieve partition " + id, e);
		}
	}

	public void putPartition(Partition<I, V, E, M> partition, int i) {
		Integer id = partition.getId();
		PutPartition pp = new PutPartition(id, partition);
		pp.setCallType(i);
		try {
			pool.submit(pp).get();
		} catch (InterruptedException e) {
			throw new IllegalStateException(
					"putPartition: cannot put back partition " + id, e);
		} catch (ExecutionException e) {
			throw new IllegalStateException(
					"putPartition: cannot put back partition " + id, e);
		}
	}

	@Override
	public void putPartition(Partition<I, V, E, M> partition) {
		Integer id = partition.getId();
		try {
			pool.submit(new PutPartition(id, partition)).get();
		} catch (InterruptedException e) {
			throw new IllegalStateException(
					"putPartition: cannot put back partition " + id, e);
		} catch (ExecutionException e) {
			throw new IllegalStateException(
					"putPartition: cannot put back partition " + id, e);
		}
	}

	@Override
	public void deletePartition(Integer id) {
		try {
			pool.submit(new DeletePartition(id)).get();
		} catch (InterruptedException e) {
			throw new IllegalStateException(
					"deletePartition: cannot delete partition " + id, e);
		} catch (ExecutionException e) {
			throw new IllegalStateException(
					"deletePartition: cannot delete partition " + id, e);
		}
	}

	@Override
	public Partition<I, V, E, M> removePartition(Integer id) {
		Partition<I, V, E, M> partition = getPartition(id);
		// we put it back, so the partition can turn INACTIVE and be deleted.
		putPartition(partition);
		deletePartition(id);
		return partition;
	}

	@Override
	public void addPartition(Partition<I, V, E, M> partition) {
		Integer id = partition.getId();
		try {
			pool.submit(new AddPartition(partition.getId(), partition)).get();
		} catch (InterruptedException e) {
			throw new IllegalStateException(
					"addPartition: cannot add partition " + id, e);
		} catch (ExecutionException e) {
			throw new IllegalStateException(
					"addPartition: cannot add partition " + id, e);
		}
	}

	@Override
	public void shutdown() {
		try {
			pool.shutdown();
			try {
				if (!pool.awaitTermination(120, TimeUnit.SECONDS)) {
					pool.shutdownNow();
				}
			} catch (InterruptedException e) {
				pool.shutdownNow();
			}
		} finally {
			for (Integer id : onDiskVertexCountMap.keySet()) {
				deletePartitionFiles(id);
			}
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(partitionIds.toString());
		sb.append("\nActive\n");
		for (Entry<Integer, Partition<I, V, E, M>> e : active.entrySet()) {
			sb.append(e.getKey() + ":" + e.getValue() + "\n");
		}
		sb.append("Inactive\n");
		for (Entry<Integer, Partition<I, V, E, M>> e : inactive.entrySet()) {
			sb.append(e.getKey() + ":" + e.getValue() + "\n");
		}
		sb.append("OnDisk\n");
		for (Entry<Integer, Integer> e : onDiskVertexCountMap.entrySet()) {
			sb.append(e.getKey() + ":" + e.getValue() + "\n");
		}
		sb.append("Counters\n");
		for (Entry<Integer, Integer> e : counters.entrySet()) {
			sb.append(e.getKey() + ":" + e.getValue() + "\n");
		}
		sb.append("Pending\n");
		for (Entry<Integer, Condition> e : pending.entrySet()) {
			sb.append(e.getKey() + "\n");
		}
		return sb.toString();
	}

	/**
	 * Increment the number of active users for a partition. Caller should hold
	 * the global write lock.
	 * 
	 * @param id
	 *            The id of the counter to increment
	 * @return The new value
	 */
	private Integer incrementCounter(Integer id) {
		Integer count = counters.get(id);
		if (count == null) {
			count = 0;
		}
		counters.put(id, ++count);
		return count;
	}

	/**
	 * Decrement the number of active users for a partition. Caller should hold
	 * the global write lock.
	 * 
	 * @param id
	 *            The id of the counter to decrement
	 * @return The new value
	 */
	private Integer decrementCounter(Integer id) {
		Integer count = counters.get(id);
		if (count == null) {
			throw new IllegalStateException("no counter for partition " + id);
		}
		counters.put(id, --count);
		if (count < 0)
			LOG.info("plz check!! count=" + count);
		return count;
	}

	/**
	 * Writes vertex data (Id, Vertex Value and halted state) to stream.
	 * 
	 * @param output
	 *            The output stream
	 * @param vertex
	 *            The vertex to serialize
	 * @throws IOException
	 */
	private void writeVertexData(DataOutput output, Vertex<I, V, E, M> vertex)
			throws IOException {
		vertex.getId().write(output);
		vertex.getValue().write(output);
		output.writeBoolean(vertex.isHalted());
	}

	/**
	 * Writes vertex edges (Id, Edges) to stream.
	 * 
	 * @param output
	 *            The output stream
	 * @param vertex
	 *            The vertex to serialize
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private void writeOutEdges(DataOutput output, Vertex<I, V, E, M> vertex)
			throws IOException {
		vertex.getId().write(output);
		if(vertex.getEdges()==null){
			output.writeInt(0);
			return;
		}
		((OutEdges<I, E>) vertex.getEdges()).write(output);
	}

	/**
	 * Read vertex data from an input and initialize the vertex.
	 * 
	 * @param in
	 *            The input stream
	 * @param vertex
	 *            The vertex to initialize
	 * @throws IOException
	 */
	private void readVertexData(DataInput in, Vertex<I, V, E, M> vertex)
			throws IOException {
		I id = conf.createVertexId();
		id.readFields(in);
		V value = conf.createVertexValue();
		value.readFields(in);
		vertex.initialize(id, value);
		if (in.readBoolean()) {
			vertex.voteToHalt();
		} else {
			vertex.wakeUp();
		}
	}

	/**
	 * Read vertex edges from an input and set them to the vertex.
	 * 
	 * @param in
	 *            The input stream
	 * @param partition
	 *            The partition owning the vertex
	 * @throws IOException
	 */
	private void readOutEdges(DataInput in, Partition<I, V, E, M> partition)
			throws IOException {
		I id = conf.createVertexId();
		id.readFields(in);
		Vertex<I, V, E, M> v = partition.getVertex(id);
		OutEdges<I, E> edges = conf.createOutEdges();
		edges.readFields(in);
		v.setEdges(edges);
	}

	private void clearEdgesOfHotPartition(Partition<I, V, E, M> partition,
			Integer id, long numVertices) {

	}

	/**
	 * used by partition that is hot and without in-mem-edges under
	 * OnlineCompute mode
	 * 
	 * @param id
	 * @param numVertices
	 * @return
	 * @throws IOException
	 */
	private Partition<I, V, E, M> loadPartitionEdges(
			Partition<I, V, E, M> partition, Integer id, int numVertices) {
		try {
			File file = new File(getEdgesPath(id));
			DataInputStream inputStream = new DataInputStream(
					new BufferedInputStream(new FileInputStream(file)));
//			LOG.info("loading edges for in memory partition" + id + " from"
//					+ getEdgesPath(id));
			for (int i = 0; i < numVertices; ++i) {
				readOutEdges(inputStream, partition);
			}
//			LOG.info("read edges finished for in memory partition " + id + " from"
//					+ getEdgesPath(id));
			inputStream.close();
			/*
			 * If the graph is static, keep the file around.
			 */
			if (!isStaticGraph) {
				file.delete();
			}
		} catch (Exception e) {

			e.printStackTrace();
		}
		return partition;
	}

	boolean repartitioned = false;

	/**
	 * Load a partition from disk. It deletes the files after the load, except
	 * for the edges, if the graph is static.
	 * 
	 * @param id
	 *            The id of the partition to load
	 * @param numVertices
	 *            The number of vertices contained on disk
	 * @return The partition
	 * @throws IOException
	 */
	private Partition<I, V, E, M> loadPartition(Integer id, int numVertices)
			throws IOException {
		Partition<I, V, E, M> partition = conf.createPartition(id, context);
		File file = new File(getVerticesPath(id));
//		if (LOG.isInfoEnabled()) {
//			LOG.info("loadPartition: reading partition vertices "
//					+ partition.getId() + ", size=" + file.length() + " from "
//					+ file.getAbsolutePath());
//		}
		DataInputStream inputStream = new DataInputStream(
				new BufferedInputStream(new FileInputStream(file)));
		for (int i = 0; i < numVertices; ++i) {
			Vertex<I, V, E, M> vertex = conf.createVertex();
			readVertexData(inputStream, vertex);
			partition.putVertex(vertex);
		}
		inputStream.close();
		file.delete();
		// if(!withEdges && hotPartitionIds!=null &&
		// hotPartitionIds.contains(id)){
		// return partition;
		// }
		file = new File(getEdgesPath(id));
		inputStream = new DataInputStream(new BufferedInputStream(
				new FileInputStream(file)));
		for (int i = 0; i < numVertices; ++i) {
			readOutEdges(inputStream, partition);
		}
		inputStream.close();
		/*
		 * If the graph is static, keep the file around.
		 */
		if (!conf.isStaticGraph()) {
			file.delete();
		}
		return partition;
	}

	/**
	 * when doing hot analysis,
	 * 
	 * @param partition
	 * @throws IOException
	 */
	private void offloadEdgeOnHotPartition(Partition<I, V, E, M> partition) {
		try {
//			if (LOG.isInfoEnabled()) {
//				LOG.info("offloadEdgeOnHotPartition: writing partition edges "
//						+ partition.getId());
//			}
			File file = new File(getEdgesPath(partition.getId()));
			file.getParentFile().mkdirs();
			/*
			 * Avoid writing back edges if we have already written them once and
			 * the graph is not changing.
			 */
			if (!conf.isStaticGraph() || !file.exists()) {
				file.createNewFile();
//				if (LOG.isInfoEnabled()) {
//					LOG.info("offloadEdges " + partition.getId()
//							+ " for the first time to "
//							+ file.getAbsolutePath());
//				}
				DataOutputStream outputStream = new DataOutputStream(
						new BufferedOutputStream(new FileOutputStream(file)));
				for (Vertex<I, V, E, M> vertex : partition) {
					writeOutEdges(outputStream, vertex);
					vertex.setEdges(null);
				}
				outputStream.close();
			} else {
//				if (LOG.isInfoEnabled()) {
//					LOG.info("is static graph and edgefile had been created before. pid= "
//							+ partition.getId());
//				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Write a partition to disk.
	 * 
	 * @param partition
	 *            The partition to offload
	 * @throws IOException
	 */
	private void offloadPartition(Partition<I, V, E, M> partition)
			throws IOException {
		File file = new File(getVerticesPath(partition.getId()));
		file.getParentFile().mkdirs();
		file.createNewFile();
//		if (LOG.isInfoEnabled()) {
//			LOG.info("offloadPartition: writing partition vertices "
//					+ partition.getId() + " to " + file.getAbsolutePath());
//		}
		DataOutputStream outputStream = new DataOutputStream(
				new BufferedOutputStream(new FileOutputStream(file)));
		for (Vertex<I, V, E, M> vertex : partition) {
			writeVertexData(outputStream, vertex);
		}
		outputStream.close();
		file = new File(getEdgesPath(partition.getId()));
		/*
		 * Avoid writing back edges if we have already written them once and the
		 * graph is not changing.
		 */
		if (!conf.isStaticGraph() || !file.exists()) {
			file.createNewFile();
//			if (LOG.isInfoEnabled()) {
//				LOG.info("first time, writing partition edges "
//						+ partition.getId() + " to " + file.getAbsolutePath());
//			}
			outputStream = new DataOutputStream(new BufferedOutputStream(
					new FileOutputStream(file)));
			for (Vertex<I, V, E, M> vertex : partition) {
				writeOutEdges(outputStream, vertex);
				vertex.setEdges(null);
			}
			outputStream.close();
		}
	}

	/**
	 * Append a partition on disk at the end of the file. Expects the caller to
	 * hold the global lock.
	 * 
	 * @param partition
	 *            The partition
	 * @throws IOException
	 */
	private void addToOOCPartition(Partition<I, V, E, M> partition)
			throws IOException {
		Integer id = partition.getId();
		Integer count = onDiskVertexCountMap.get(id);
		Long edgeCount = edgeCountMap.get(id);
		edgeCountMap.put(id, edgeCount + partition.getEdgeCount());
		Long vertexCount = vertexCountMap.get(id);
		vertexCountMap.put(id, vertexCount + partition.getVertexCount());
		onDiskVertexCountMap.put(id, count + (int) partition.getVertexCount());
		File file = new File(getVerticesPath(id));
		DataOutputStream outputStream = new DataOutputStream(
				new BufferedOutputStream(new FileOutputStream(file, true)));
		for (Vertex<I, V, E, M> vertex : partition) {
			writeVertexData(outputStream, vertex);
		}
		outputStream.close();
		file = new File(getEdgesPath(id));
		outputStream = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(file, true)));
		for (Vertex<I, V, E, M> vertex : partition) {
			writeOutEdges(outputStream, vertex);
			vertex.setEdges(null);
		}
		outputStream.close();
	}

	/**
	 * Delete a partition's files.
	 * 
	 * @param id
	 *            The id of the partition owning the file.
	 */
	public void deletePartitionFiles(Integer id) {
		File file = new File(getVerticesPath(id));
		file.delete();
		file = new File(getEdgesPath(id));
		file.delete();
	}

	/**
	 * Get the path and basename of the storage files.
	 * 
	 * @param partitionId
	 *            The partition
	 * @return The path to the given partition
	 */
	private String getPartitionPath(Integer partitionId) {
		int hash = hasher.hashInt(partitionId).asInt();
		int idx = Math.abs(hash % basePaths.length);
		return basePaths[idx] + "/partition-" + partitionId;
	}

	private String getRePartitionPath(Integer partitionId) {
		int hash = hasher.hashInt(partitionId).asInt();
		int idx = Math.abs(hash % basePaths.length);
		return basePaths[idx] + "/repartition-" + partitionId;
	}

	/**
	 * Get the path to the file where vertices are stored.
	 * 
	 * @param partitionId
	 *            The partition
	 * @return The path to the vertices file
	 */
	private String getVerticesPath(Integer partitionId) {
		if (repartitioned)
			return getRePartitionPath(partitionId) + "_vertices";
		else
			return getPartitionPath(partitionId) + "_vertices";
	}

	/**
	 * Get the path to the file where edges are stored.
	 * 
	 * @param partitionId
	 *            The partition
	 * @return The path to the edges file
	 */
	private String getEdgesPath(Integer partitionId) {
		if (repartitioned)
			return getRePartitionPath(partitionId) + "_edges";
		else
			return getPartitionPath(partitionId) + "_edges";
	}

	/**
	 * Task that gets a partition from the store
	 */
	private class GetPartition implements Callable<Partition<I, V, E, M>> {
		/** Partition id */
		private Integer id;

		/**
		 * Constructor
		 * 
		 * @param id
		 *            Partition id
		 */
		public GetPartition(Integer id) {
			this.id = id;
		}

		public void setCallType(int i) {
			if (edgeSeparation)
				callType = i;
			else
				callType = 0;
		}

		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append("getPartition id=" + id + " callType=" + callType);
			sb.append("\nStates\n");
			for (Entry<Integer, State> e : states.entrySet()) {
				sb.append(e.getKey() + ":" + e.getValue() + "\n");
			}
			sb.append("\nActive\n");
			for (Entry<Integer, Partition<I, V, E, M>> e : active.entrySet()) {
				sb.append(e.getKey() + ":" + e.getValue() + "\n");
			}
			sb.append("Inactive\n");
			for (Entry<Integer, Partition<I, V, E, M>> e : inactive.entrySet()) {
				sb.append(e.getKey() + ":" + e.getValue() + "\n");
			}
			sb.append("OnDisk\n");
			for (Entry<Integer, Integer> e : onDiskVertexCountMap.entrySet()) {
				sb.append(e.getKey() + ":" + e.getValue() + "\n");
			}
			sb.append("Counters\n");
			for (Entry<Integer, Integer> e : counters.entrySet()) {
				sb.append(e.getKey() + ":" + e.getValue() + "\n");
			}
			sb.append("inactiveColdPartitions\n");
			for (Entry<Integer, Partition<I, V, E, M>> e : inactiveColdPartitions
					.entrySet()) {
				sb.append(e.getKey() + "\n");
			}
			return sb.toString();
		}

		/**
		 * callType = 0 : normal(default) callType = 1 : load edges to hot
		 * partition;
		 */
		public int callType = 0;

		/**
		 * Removes and returns the last recently used entry.
		 * 
		 * @return The last recently used entry.
		 */
		private Entry<Integer, Partition<I, V, E, M>> getLRUEntry() {
			Iterator<Entry<Integer, Partition<I, V, E, M>>> i;
			Entry<Integer, Partition<I, V, E, M>> lruEntry;
			if (inactiveColdPartitions != null) {
				i = inactiveColdPartitions.entrySet().iterator();
				lruEntry = i.next();
				i.remove();
				inactive.remove(lruEntry.getKey());
			} else {
				i = inactive.entrySet().iterator();
				lruEntry = i.next();
				i.remove();
			}
			return lruEntry;
		}

		@Override
		public Partition<I, V, E, M> call() throws Exception {
			Partition<I, V, E, M> partition = null;

			while (partition == null) {
				wLock.lock();
				try {
					State pState = states.get(id);
					switch (pState) {
					case ONDISK:
						Entry<Integer, Partition<I, V, E, M>> lru = null;
						states.put(id, State.LOADING);
						int numVertices = onDiskVertexCountMap.remove(id);
						/*
						 * Wait until we have space in memory or inactive cold
						 * data for a switch
						 */
						while (inMemoryPartitions >= maxInMemoryPartitions
								&& inactiveColdPartitions.size() == 0) {
							// inactive.size() == 0) {
							notEmpty.await();
						}
						/*
						 * we have to make some space first
						 */
						if (inMemoryPartitions >= maxInMemoryPartitions) {
							lru = getLRUEntry();
							states.put(lru.getKey(), State.OFFLOADING);
							pending.get(lru.getKey()).signalAll();
						} else { // there is space, just add it to the in-memory
									// partitions
							inMemoryPartitions++;
						}
						/*
						 * do IO without contention, the threads interested to
						 * these partitions will subscribe to the relative
						 * Condition.
						 */
						wLock.unlock();
						if (lru != null) {
							offloadPartition(lru.getValue());
						}
						partition = loadPartition(id, numVertices);
						wLock.lock();
						/*
						 * update state and signal the pending threads
						 */
						if (lru != null) {
							states.put(lru.getKey(), State.ONDISK);
							onDiskVertexCountMap.put(lru.getKey(), (int) lru
									.getValue().getVertexCount());
							pending.get(lru.getKey()).signalAll();
						}
						active.put(id, partition);
						states.put(id, State.ACTIVE);
						pending.get(id).signalAll();
						incrementCounter(id);
						break;
					case INACTIVE:
						if (hotPartitionIds != null
								&& !hotPartitionIds.contains(id)) {
							inactiveColdPartitions.remove(id);
						}
						partition = inactive.remove(id);
						incrementCounter(id);
						if (callType == 1) {
							states.put(id, State.LOADING);
							// wLock.unlock();
							loadPartitionEdges(partition, id,
									(int) partition.getVertexCount());
//							LOG.info("start");
//							LOG.info("end");
							// wLock.lock();
//							LOG.info("acquire lock again" + id);
						}
						active.put(id, partition);
						states.put(id, State.ACTIVE);
						pending.get(id).signalAll();
						break;
					case ACTIVE:
						partition = active.get(id);
						incrementCounter(id);
						if (callType == 1) {
							states.put(id, State.LOADING);
							// wLock.unlock();
							loadPartitionEdges(partition, id,
									(int) partition.getVertexCount());
							// wLock.lock();
							states.put(id, State.ACTIVE);
							pending.get(id).signalAll();
						}
						break;
					case LOADING:
						pending.get(id).await();
						break;
					case OFFLOADING:
						pending.get(id).await();
						break;
					default:
						throw new IllegalStateException("illegal state "
								+ pState + " for partition " + id);
					}
				} finally {
					wLock.unlock();
				}
			}
			return partition;
		}
	}

	/**
	 * Task that puts a partition back to the store
	 */
	private class PutPartition implements Callable<Void> {
		/** Partition id */
		private Integer id;

		/**
		 * Constructor
		 * 
		 * @param id
		 *            The partition id
		 * @param partition
		 *            The partition
		 */
		public PutPartition(Integer id, Partition<I, V, E, M> partition) {
			this.id = id;
		}

		int callType = 0;

		/**
		 * callType=0:normal callType=1(only for hot partitions):clear edges
		 * 
		 * @param i
		 */
		public void setCallType(int i) {
			if (edgeSeparation && hotPartitionIds != null
					&& hotPartitionIds.contains(id))
				callType = i;
			else
				callType = 0;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("putPartition id=" + id + " callType=" + callType);
			sb.append("\nStates\n");
			for (Entry<Integer, State> e : states.entrySet()) {
				sb.append(e.getKey() + ":" + e.getValue() + "\n");
			}
			sb.append("\nActive\n");
			for (Entry<Integer, Partition<I, V, E, M>> e : active.entrySet()) {
				sb.append(e.getKey() + ":" + e.getValue() + "\n");
			}
			sb.append("Inactive\n");
			for (Entry<Integer, Partition<I, V, E, M>> e : inactive.entrySet()) {
				sb.append(e.getKey() + ":" + e.getValue() + "\n");
			}
			sb.append("OnDisk\n");
			for (Entry<Integer, Integer> e : onDiskVertexCountMap.entrySet()) {
				sb.append(e.getKey() + ":" + e.getValue() + "\n");
			}
			sb.append("Counters\n");
			for (Entry<Integer, Integer> e : counters.entrySet()) {
				sb.append(e.getKey() + ":" + e.getValue() + "\n");
			}
			sb.append("inactiveColdPartitions\n");
			for (Entry<Integer, Partition<I, V, E, M>> e : inactiveColdPartitions
					.entrySet()) {
				sb.append(e.getKey() + "\n");
			}
			return sb.toString();
		}

		@Override
		public Void call() throws Exception {
			wLock.lock();
			try {
				Partition<I, V, E, M> p;
				if (decrementCounter(id) == 0) {
					p = active.remove(id);
					if (p == null) {
						LOG.info("partition is null!!!");
						LOG.info(this.toString());
					}
					if (callType == 1) {
//						LOG.info("!!clear edges for partition " + id);
						states.put(id, State.OFFLOADING);
						// pending.get(id).signalAll();
						// wLock.unlock();
						offloadEdgeOnHotPartition(p);
						// wLock.lock();
					}
					inactive.put(id, p);
					if (hotPartitionIds != null
							&& !hotPartitionIds.contains(id)) {
						inactiveColdPartitions.put(id, p);
					}
					states.put(id, State.INACTIVE);
					pending.get(id).signalAll();
					notEmpty.signal();
				} else {
					if (callType == 1) {
						p = active.get(id);
//						LOG.info("clear edges for partition " + id);
						if (p == null) {
							LOG.info("partition is null!!!");
							LOG.info(this.toString());
						}
						states.put(id, State.OFFLOADING);
						// pending.get(id).signalAll();
						// wLock.unlock();
						offloadEdgeOnHotPartition(p);
						for (Vertex<I, V, E, M> v : p) {
							v.setEdges(conf.createOutEdges());
						}
						// wLock.lock();
						states.put(id, State.ACTIVE);
						pending.get(id).signalAll();
					}
				}
				return null;
			} finally {
				wLock.unlock();
			}
		}
	}

	/**
	 * Task that adds a partition to the store
	 */
	private class AddPartition implements Callable<Void> {
		/** Partition id */
		private Integer id;
		/** Partition */
		private Partition<I, V, E, M> partition;

		/**
		 * Constructor
		 * 
		 * @param id
		 *            The partition id
		 * @param partition
		 *            The partition
		 */
		public AddPartition(Integer id, Partition<I, V, E, M> partition) {
			this.id = id;
			this.partition = partition;
		}

		@Override
		public Void call() throws Exception {

			wLock.lock();
			try {
				if (partitionIds.contains(id)) {
					Partition<I, V, E, M> existing = null;
					// is out of core flag
					boolean isOOC = false;
					boolean done = false;
					while (!done) {
						State pState = states.get(id);
						switch (pState) {
						case ONDISK:
							isOOC = true;
							done = true;
							break;
						/*
						 * just add data to the in-memory partitions,
						 * concurrency should be managed by the caller.
						 */
						case INACTIVE:
							existing = inactive.get(id);
							done = true;
							break;
						case ACTIVE:
							existing = active.get(id);
							done = true;
							break;
						case LOADING:
							pending.get(id).await();
							break;
						case OFFLOADING:
							pending.get(id).await();
							break;
						default:
							throw new IllegalStateException("illegal state "
									+ pState + " for partition " + id);
						}
					}
					if (isOOC) {
						addToOOCPartition(partition);
					} else {
						Long edgeCount = edgeCountMap.get(id);
						edgeCountMap.put(id,
								edgeCount + partition.getEdgeCount());
						Long vertexCount = vertexCountMap.get(id);
						vertexCountMap.put(id,
								vertexCount + partition.getVertexCount());
						if (conf.useEdgeSeparation()) {
							((SimplePartition) existing).addPartition(
									partition, getEdgesPath(id));
						} else
							existing.addPartition(partition);
					}
				} else {
					Condition newC = wLock.newCondition();
					pending.put(id, newC);
					partitionIds.add(id);
					edgeCountMap.put(id, partition.getEdgeCount());
					vertexCountMap.put(id, partition.getVertexCount());
					if (inMemoryPartitions < maxInMemoryPartitions) {
						inMemoryPartitions++;
						//
						if (edgeSeparation)
							offloadEdgeOnHotPartition(partition);
						states.put(id, State.INACTIVE);
						inactive.put(id, partition);
						pending.get(id).signalAll();
						if (hotPartitionIds != null
								&& !hotPartitionIds.contains(id)) {
							inactiveColdPartitions.put(id, partition);
						}
						notEmpty.signal();
					} else {
						states.put(id, State.OFFLOADING);
						onDiskVertexCountMap.put(id,
								(int) partition.getVertexCount());
						wLock.unlock();
						offloadPartition(partition);
						wLock.lock();
						states.put(id, State.ONDISK);
						newC.signalAll();
					}
				}
				return null;
			} finally {
				wLock.unlock();
			}
		}
	}

	/**
	 * Task that deletes a partition to the store
	 */
	private class DeletePartition implements Callable<Void> {
		/** Partition id */
		private Integer id;

		/**
		 * Constructor
		 * 
		 * @param id
		 *            The partition id
		 */
		public DeletePartition(Integer id) {
			this.id = id;
		}

		@Override
		public Void call() throws Exception {
			boolean done = false;

			wLock.lock();
			try {
				while (!done) {
					State pState = states.get(id);
					switch (pState) {
					case ONDISK:
						onDiskVertexCountMap.remove(id);
						deletePartitionFiles(id);
						done = true;
						break;
					case INACTIVE:
						inactive.remove(id);
						if (hotPartitionIds != null
								&& !hotPartitionIds.contains(id)) {
							inactiveColdPartitions.remove(id);
						}
						inMemoryPartitions--;
						notEmpty.signal();
						done = true;
						break;
					case ACTIVE:
						pending.get(id).await();
						break;
					case LOADING:
						pending.get(id).await();
						break;
					case OFFLOADING:
						pending.get(id).await();
						break;
					default:
						throw new IllegalStateException("illegal state "
								+ pState + " for partition " + id);
					}
				}
				partitionIds.remove(id);
				states.remove(id);
				counters.remove(id);
				pending.remove(id).signalAll();
				return null;
			} finally {
				wLock.unlock();
			}
		}
	}

	/**
	 * Direct Executor that executes tasks within the calling threads.
	 */
	private class DirectExecutorService extends AbstractExecutorService {
		/** Executor state */
		private volatile boolean shutdown = false;

		/**
		 * 
		 * Constructor
		 */
		public DirectExecutorService() {
		}

		/**
		 * Execute the task in the calling thread.
		 * 
		 * @param task
		 *            Task to execute
		 */
		public void execute(Runnable task) {
			task.run();
		}

		/**
		 * Shutdown the executor.
		 */
		public void shutdown() {
			this.shutdown = true;
		}

		/**
		 * Shutdown the executor and return the current queue (empty).
		 * 
		 * @return The list of awaiting tasks
		 */
		public List<Runnable> shutdownNow() {
			this.shutdown = true;
			return Collections.emptyList();
		}

		/**
		 * Return current shutdown state.
		 * 
		 * @return Shutdown state
		 */
		public boolean isShutdown() {
			return shutdown;
		}

		/**
		 * Return current termination state.
		 * 
		 * @return Termination state
		 */
		public boolean isTerminated() {
			return shutdown;
		}

		/**
		 * Do nothing and return shutdown state.
		 * 
		 * @param timeout
		 *            Timeout
		 * @param unit
		 *            Time unit
		 * @return Shutdown state
		 */
		public boolean awaitTermination(long timeout, TimeUnit unit)
				throws InterruptedException {
			return shutdown;
		}
	}

	public int getRePartitionId(I vertexId) {
		Integer res = lookUp.get(vertexId);
		return lookUp.get(vertexId);
	}

	public Partition<I, V, E, M> getPartitionIfIsHot(int pid) {
		if (hotPartitionIds.contains(pid))
			return getPartition(pid);
		else
			return null;
	}

	public boolean isHot(int pid) {
		return hotPartitionIds.contains(pid);
	}

	/**
	 * return partition only if it's already in memory otherwise return null
	 * 
	 * @param pid
	 * @return
	 */
	public Partition<I, V, E, M> getPartitionIfInMemory(int pid) {
		wLock.lock();
		try {
			State state = states.get(pid);
			if (state == State.ACTIVE || state == State.INACTIVE) {
				return getPartition(pid);
			} else
				return null;
		} finally {
			wLock.unlock();
		}
	}

	public long getVertexCount(int partitionId) {
		return vertexCountMap.get(partitionId);
	}

	public long getEdgeCount(int partitionId) {
		return edgeCountMap.get(partitionId);
	}

}
