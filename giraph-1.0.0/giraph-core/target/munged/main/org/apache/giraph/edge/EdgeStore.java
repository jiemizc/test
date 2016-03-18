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

package org.apache.giraph.edge;

import com.google.common.collect.MapMaker;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.utils.ByteArrayVertexIdEdges;
import org.apache.giraph.utils.CallableFactory;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

/**
 * Collects incoming edges for vertices owned by this worker.
 * 
 * @param <I>
 *            Vertex id
 * @param <V>
 *            Vertex value
 * @param <E>
 *            Edge value
 * @param <M>
 *            Message data
 */
public class EdgeStore<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable> {
	/** Class logger */
	protected static final Logger LOG = Logger.getLogger(EdgeStore.class);
	/** Service worker. */
	protected CentralizedServiceWorker<I, V, E, M> service;
	/** Giraph configuration. */
	protected ImmutableClassesGiraphConfiguration<I, V, E, M> configuration;
	/** Progressable to report progress. */
	protected Progressable progressable;
	/** Map used to temporarily store incoming edges. */
	protected ConcurrentMap<Integer, ConcurrentMap<I, OutEdges<I, E>>> transientEdges;
	// protected ConcurrentMap<I,
	// PairList<I,E>> srcvid2dstvids;
	// two-tier map is much more efficient
	protected ConcurrentMap<Integer, ConcurrentMap<I, PairList<I, E>>> srcvid2outedges;
	protected ConcurrentMap<Integer, ConcurrentMap<I, List<I>>> srcvid2dstvids;
	
	/**
	 * Whether the chosen {@link OutEdges} implementation allows for Edge reuse.
	 */
	protected boolean reuseEdgeObjects;

	/**
	 * Whether run in incomingEdgeMode
	 */
	protected boolean incomingEdgeMode;
	/**
	 * Whether the {@link OutEdges} class used during input is different from
	 * the one used during computation.
	 */
	protected boolean useInputOutEdges;

	protected boolean useSrc2dstEdges = false;
	/**
	 * Constructor.
	 * 
	 * @param service
	 *            Service worker
	 * @param configuration
	 *            Configuration
	 * @param progressable
	 *            Progressable
	 */
	public EdgeStore(CentralizedServiceWorker<I, V, E, M> service,
			ImmutableClassesGiraphConfiguration<I, V, E, M> configuration,
			Progressable progressable) {
		this.service = service;
		this.configuration = configuration;
		this.progressable = progressable;
		transientEdges = new MapMaker().concurrencyLevel(
				configuration.getNettyServerExecutionConcurrency()).makeMap();
		reuseEdgeObjects = configuration.reuseEdgeObjects();
		useInputOutEdges = configuration.useInputOutEdges();
		if (incomingEdgeMode)
		{
			if (useSrc2dstEdges)
				srcvid2outedges = new MapMaker().concurrencyLevel(
						configuration.getNettyServerExecutionConcurrency())
						.makeMap();
			else
				srcvid2dstvids = new MapMaker().concurrencyLevel(
					configuration.getNettyServerExecutionConcurrency())
					.makeMap();
		}
	}

	public void addToOutEdgeMap(Partition<I, V, E, M> partition) {
		for (Vertex<I, V, E, M> vertex : partition) {
			I dstId = vertex.getId();
			// these are actually incoming edges
			for (Edge<I, E> edge : vertex.getEdges()) {
				I srcId = edge.getTargetVertexId();
				Integer partitionId = service.getPartitionId(srcId);
				I srcIdCopy = org.apache.hadoop.io.WritableUtils.clone(srcId,
						configuration);
				if(useSrc2dstEdges){
					E edgeValue = edge.getValue();
					E edgeValueCopy = org.apache.hadoop.io.WritableUtils.clone(
							edgeValue, configuration);
					ConcurrentMap<I, PairList<I, E>> partitionEdges = srcvid2outedges
						.get(partitionId);
					if (partitionEdges == null) {
						ConcurrentMap<I, PairList<I, E>> newPartitionEdges = new MapMaker()
								.concurrencyLevel(
										configuration
												.getNettyServerExecutionConcurrency())
								.makeMap();
						partitionEdges = srcvid2outedges.putIfAbsent(partitionId,
								newPartitionEdges);
						if (partitionEdges == null) {
							partitionEdges = newPartitionEdges;
						}
					}
					PairList<I, E> edgeList = partitionEdges.get(srcIdCopy);
					if (edgeList == null) {
						edgeList = new PairList<I, E>();
						edgeList.initialize();
						PairList<I, E> prev = partitionEdges.putIfAbsent(srcIdCopy,
								edgeList);
						// if prev==null, edgeList is in the map
						// else prev already exists, so no need to put new list any
						// more
						edgeList = prev == null ? edgeList : prev;
					}
					synchronized (edgeList) {
						edgeList.add(dstId, edgeValueCopy);
					}
				}else{
					ConcurrentMap<I, List<I>> partitionEdges = srcvid2dstvids
							.get(partitionId);
						if (partitionEdges == null) {
							ConcurrentMap<I, List<I>> newPartitionEdges = new MapMaker()
									.concurrencyLevel(
											configuration
													.getNettyServerExecutionConcurrency())
									.makeMap();
							partitionEdges = srcvid2dstvids.putIfAbsent(partitionId,
									newPartitionEdges);
							if (partitionEdges == null) {
								partitionEdges = newPartitionEdges;
							}
						}
						List<I> dstIdList = partitionEdges.get(srcIdCopy);
						if (dstIdList == null) {
							dstIdList = new ArrayList<I>();
							List<I> prev = partitionEdges.putIfAbsent(srcIdCopy,
									dstIdList);
							// if prev==null, edgeList is in the map
							// else prev already exists, so no need to put new list any
							// more
							dstIdList = prev == null ? dstIdList : prev;
						}
						synchronized (dstIdList) {
							dstIdList.add(dstId);
						}
				}
			}
		}
	}

	/**
	 * Add edges belonging to a given partition on this worker. Note: This
	 * method is thread-safe.
	 * 
	 * @param partitionId
	 *            Partition id for the incoming edges.
	 * @param edges
	 *            Incoming edges
	 */
	public void addPartitionEdges(int partitionId,
			ByteArrayVertexIdEdges<I, E> edges) {
		ConcurrentMap<I, OutEdges<I, E>> partitionEdges = transientEdges
				.get(partitionId);
		if (partitionEdges == null) {
			ConcurrentMap<I, OutEdges<I, E>> newPartitionEdges = new MapMaker()
					.concurrencyLevel(
							configuration.getNettyServerExecutionConcurrency())
					.makeMap();
			partitionEdges = transientEdges.putIfAbsent(partitionId,
					newPartitionEdges);
			if (partitionEdges == null) {
				partitionEdges = newPartitionEdges;
			}
		}
		ByteArrayVertexIdEdges<I, E>.VertexIdEdgeIterator vertexIdEdgeIterator = edges
				.getVertexIdEdgeIterator();
		while (vertexIdEdgeIterator.hasNext()) {
			vertexIdEdgeIterator.next();
			I vertexId = vertexIdEdgeIterator.getCurrentVertexId();
			Edge<I, E> edge = reuseEdgeObjects ? vertexIdEdgeIterator
					.getCurrentEdge() : vertexIdEdgeIterator
					.releaseCurrentEdge();
			OutEdges<I, E> outEdges = partitionEdges.get(vertexId);
			if (outEdges == null) {
				OutEdges<I, E> newOutEdges = configuration
						.createAndInitializeInputOutEdges();
				outEdges = partitionEdges.putIfAbsent(vertexId, newOutEdges);
				if (outEdges == null) {
					outEdges = newOutEdges;
					// Since we had to use the vertex id as a new key in the
					// map,
					// we need to release the object.
					vertexIdEdgeIterator.releaseCurrentVertexId();
				}
			}
			synchronized (outEdges) {
				outEdges.add(edge);
			}
		}
	}

	/**
	 * Convert the input edges to the {@link OutEdges} data structure used for
	 * computation (if different).
	 * 
	 * @param inputEdges
	 *            Input edges
	 * @return Compute edges
	 */
	protected OutEdges<I, E> convertInputToComputeEdges(OutEdges<I, E> inputEdges) {
		if (!useInputOutEdges) {
			return inputEdges;
		} else {
			OutEdges<I, E> computeEdges = configuration
					.createAndInitializeOutEdges(inputEdges.size());
			for (Edge<I, E> edge : inputEdges) {
				computeEdges.add(edge);
			}
			return computeEdges;
		}
	}

	/**
	 * Move all edges from temporary storage to their source vertices. Note:
	 * this method is not thread-safe.
	 */
	public void moveEdgesToVertices() {
		if (transientEdges.isEmpty()) {
			if (LOG.isInfoEnabled()) {
				LOG.info("moveEdgesToVertices: No edges to move");
			}
			return;
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("moveEdgesToVertices: Moving incoming edges to vertices.");
		}

		final BlockingQueue<Integer> partitionIdQueue = new ArrayBlockingQueue<Integer>(
				transientEdges.size());
		partitionIdQueue.addAll(transientEdges.keySet());
		int numThreads = configuration.getNumInputSplitsThreads();

		CallableFactory<Void> callableFactory = new CallableFactory<Void>() {
			@Override
			public Callable<Void> newCallable(int callableId) {
				return new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						Integer partitionId;
						while ((partitionId = partitionIdQueue.poll()) != null) {
							Partition<I, V, E, M> partition = service
									.getPartitionStore().getPartition(
											partitionId);
							ConcurrentMap<I, OutEdges<I, E>> partitionEdges = transientEdges
									.remove(partitionId);
							for (I vertexId : partitionEdges.keySet()) {
								OutEdges<I, E> outEdges = convertInputToComputeEdges(partitionEdges
										.remove(vertexId));
								Vertex<I, V, E, M> vertex = partition
										.getVertex(vertexId);
								// If the source vertex doesn't exist, create
								// it. Otherwise,
								// just set the edges.
								if (vertex == null) {
									vertex = configuration.createVertex();
									vertex.initialize(vertexId,
											configuration.createVertexValue(),
											outEdges);
									partition.putVertex(vertex);
								} else {
									vertex.setEdges(outEdges);
									// Some Partition implementations (e.g.
									// ByteArrayPartition)
									// require us to put back the vertex after
									// modifying it.
									partition.saveVertex(vertex);
								}
							}
							// Some PartitionStore implementations
							// (e.g. DiskBackedPartitionStore) require us to put
							// back the
							// partition after modifying it.
							service.getPartitionStore().putPartition(partition);
						}
						return null;
					}
				};
			}
		};
		ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
				"move-edges-%d", progressable);

		transientEdges.clear();

		if (LOG.isInfoEnabled()) {
			LOG.info("moveEdgesToVertices: Finished moving incoming edges to "
					+ "vertices.");
		}
	}

	public Map<I, PairList<I, E>> getOutEdgePartition(Integer partitionId) {
		return srcvid2outedges.get(partitionId);
	}
	public Map<I, List<I>> getDstVidPartition(Integer partitionId) {
		return srcvid2dstvids.get(partitionId);
	}
	public List<I> getDstVidsList(I srcId){
		Map<I, List<I>> edgeList = srcvid2dstvids.get(service
				.getPartitionId(srcId));
		if (edgeList == null)
			return null;
		else
			return edgeList.get(srcId);
	}
	
	public PairList<I, E> getOutEdgeList(I srcId) {
		Map<I, PairList<I, E>> edgeList = srcvid2outedges.get(service
				.getPartitionId(srcId));
		if (edgeList == null)
			return null;
		else
			return edgeList.get(srcId);
	}
}
