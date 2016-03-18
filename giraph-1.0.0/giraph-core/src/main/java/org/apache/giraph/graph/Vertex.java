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

package org.apache.giraph.graph;

import com.google.common.collect.UnmodifiableIterator;

import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MultiRandomAccessOutEdges;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.edge.MutableEdgesIterable;
import org.apache.giraph.edge.MutableEdgesWrapper;
import org.apache.giraph.edge.MutableOutEdges;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.edge.StrictRandomAccessOutEdges;
import org.apache.giraph.partition.PartitionContext;
import org.apache.giraph.worker.WorkerAggregatorUsage;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.mortbay.log.Log;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Basic abstract class for writing a BSP application for computation. Giraph
 * will store Vertex value and edges, hence all user data should be stored as
 * part of the vertex value.
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
public abstract class Vertex<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable>
		extends DefaultImmutableClassesGiraphConfigurable<I, V, E, M> implements
		WorkerAggregatorUsage {
	/** Vertex id. */
	private I id;
	/** Vertex value. */
	private V value;
	/** Outgoing edges. */
	private OutEdges<I, E> edges;
	/** If true, do not do anymore computation on this vertex. */
	private boolean halt;
	/** Global graph state **/
	private GraphState<I, V, E, M> graphState;
	/**
	 * Vertex value for online compute usage when using online compute, two
	 * online compute strategies: a)deprecated. 1. it must be copied from #value
	 * once the vertex is loaded in memory 2. it must become #value before the
	 * vertex is flushed b)current strategy 1. it must be copied to #value
	 * before next superstep starts computing.(at that point two values have
	 * same data) 2. hot partition never flushed out. 3. cold partition do not
	 * use online compute. 3. so when cold partition flushed out and load, it
	 * ignores this value
	 **/
	private V valueForOnlineCompute;
	public static Boolean useNewVersionValue = null;
	static Lock lock = new ReentrantLock();

	public boolean checkWhetherUseNewVersionValue() {
		if (useNewVersionValue == null) {
			lock.lock();
			try {
				if (useNewVersionValue == null)
					useNewVersionValue = (getConf().useOnlineCompute() && getConf()
							.isInSynchronousMode());
			} finally {
				lock.unlock();
			}
		}
		return useNewVersionValue.booleanValue();
	}

	public V getValueForOnlineCompute(long superstep) {
		return superstep % 2 == 0 ? valueForOnlineCompute : value;
	}

	public void setValueForOnlineCompute(V valueForOnlineCompute) {
		this.valueForOnlineCompute = valueForOnlineCompute;
	}

	/**
	 * Initialize id, value, and edges. This method (or the alternative form
	 * initialize(id, value)) must be called after instantiation, unless
	 * readFields() is called.
	 *
	 * @param id
	 *            Vertex id
	 * @param value
	 *            Vertex value
	 * @param edges
	 *            Iterable of edges
	 */
	public void initialize(I id, V value, Iterable<Edge<I, E>> edges) {
		this.id = id;
		this.value = value;
		checkWhetherUseNewVersionValue();
		setEdges(edges);
	}

	/**
	 * Initialize id and value. Vertex edges will be empty. This method (or the
	 * alternative form initialize(id, value, edges)) must be called after
	 * instantiation, unless readFields() is called.
	 *
	 * @param id
	 *            Vertex id
	 * @param value
	 *            Vertex value
	 */
	public void initialize(I id, V value) {
		this.id = id;
		this.value = value;
		checkWhetherUseNewVersionValue();
		this.edges = getConf().createAndInitializeOutEdges(0);
	}

	/**
	 * Set the outgoing edges for this vertex.
	 *
	 * @param edges
	 *            Iterable of edges
	 */
	public void setEdges(Iterable<Edge<I, E>> edges) {
		// If the iterable is actually an instance of OutEdges,
		// we simply take the reference.
		// Otherwise, we initialize a new OutEdges.
		if (edges == null) {
			this.edges = null;
			return;
		}
		if (edges instanceof OutEdges) {
			this.edges = (OutEdges<I, E>) edges;
		} else {
			this.edges = getConf().createAndInitializeOutEdges(edges);
		}
	}

	/**
	 * in mocgraph, we simulate raw giraph behavior by combining
	 * computeSingleMessage and sendMessage.
	 *
	 * @param messages
	 *            Messages that were sent to this vertex in the previous
	 *            superstep. Each message is only guaranteed to have a life
	 *            expectancy as long as next() is not called.
	 * @throws IOException
	 */
	public void compute(Iterable<M> messages) throws IOException {
		for (M msg : messages) {
			computeSingleMsg(value, msg, getSuperstep());
		}
		sendMessages(value, getSuperstep());
	}

	/**
	 * not for user
	 * 
	 * @param vertex
	 * @param currentData
	 * @param superstep
	 */
	public synchronized void onlineCompute(M currentData, long superstep) {
		if (!useNewVersionValue) {
			computeSingleMsg(getValue(), currentData, superstep);
		} else {
			V newValue = getValueForOnlineCompute(superstep);
			if (newValue == null) {
				newValue = createNewValue(getValue(superstep), superstep);
				this.setValueForOnlineCompute(superstep, newValue);
			}
			computeSingleMsg(newValue, currentData, superstep);
		}
	}

	public synchronized void onlineComputeForOOCVertex(M currentData,
			long superstep) {
		computeSingleMsg(getValue(), currentData, superstep);
	}


	/**
	 * when using online compute, user must supply this function to consume a
	 * single msg
	 * 
	 * @param msg
	 */
	public void computeSingleMsg(V value, M msg, long superstepNum) {
		// This method needs to be implemented if using mocgraph
	}

	/**
	 * when using online compute, user must supply this function to send msgs
	 * based on its current value. note that getValue() method cannot be used in
	 * mocgraph. please use the value we supply instead.
	 * 
	 * @param msg
	 */
	public void sendMessages(V value, long superstep) {
		// This method needs to be implemented if using mocgraph
	};

	/**
	 * when running with mocgraph synchronous mode, user may want to specify how
	 * to create the vertex value for updating.
	 * 
	 * @param oldValue
	 * @return
	 */
	public V createNewValue(V oldValue, long superstep) {
		return getConf().createVertexValue();
	}

	public synchronized void sendMessagesForInmemVertex(long superstep) {
		if (!useNewVersionValue) {
			sendMessages(getValue(), superstep);
		} else {
			V oldValue = getValue(superstep);
			V newValue = getValueForOnlineCompute(superstep);

			sendMessages(oldValue, superstep);
			if (newValue == null) {
				newValue = createNewValue(oldValue, superstep);
				setValueForOnlineCompute(superstep, newValue);
			}
			setValue(superstep, null);
		}
	}

	public synchronized void sendMessagesForOOCVertex(long superstep) {
		sendMessages(getValue(), superstep);
	}

	private void setValue(long superstep, V oldValue) {
		if (superstep % 2 == 0)
			value = oldValue;
		else
			valueForOnlineCompute = oldValue;
	}

	private void setValueForOnlineCompute(long superstep, V newValue) {
		if (superstep % 2 == 0)
			valueForOnlineCompute = newValue;
		else
			value = newValue;
	}

	/**
	 * Retrieves the current superstep.
	 *
	 * @return Current superstep
	 */
	public long getSuperstep() {
		return graphState.getSuperstep();
	}

	/**
	 * Get the vertex id.
	 *
	 * @return My vertex id.
	 */
	public I getId() {
		return id;
	}

	/**
	 * Get the vertex value (data stored with vertex)
	 *
	 * @return Vertex value
	 */
	public V getValue() {
		return value;
	}

	/**
	 * only called by synchronous engine
	 * 
	 * @param superstep
	 * @return
	 */
	public V getValue(long superstep) {
		return superstep % 2 == 0 ? value : valueForOnlineCompute;
	}

	/**
	 * get the final vertex value for output usage when running in synchronous
	 * mode
	 * 
	 * @param superstep
	 * @return
	 */
	public V getFinalValue() {
		if (useNewVersionValue) {
			if (valueForOnlineCompute != null)
				return valueForOnlineCompute;
			else
				return value;
		} else
			return value;
	}

	/**
	 * Set the vertex data (immediately visible in the computation)
	 *
	 * @param value
	 *            Vertex data to be set
	 */
	public void setValue(V value) {
		this.value = value;
	}

	/**
	 * Get the total (all workers) number of vertices that existed in the
	 * previous superstep.
	 *
	 * @return Total number of vertices (-1 if first superstep)
	 */
	public long getTotalNumVertices() {
		return graphState.getTotalNumVertices();
	}

	/**
	 * Get the total (all workers) number of edges that existed in the previous
	 * superstep.
	 *
	 * @return Total number of edges (-1 if first superstep)
	 */
	public long getTotalNumEdges() {
		return graphState.getTotalNumEdges();
	}

	/**
	 * Get a read-only view of the out-edges of this vertex. Note: edge objects
	 * returned by this iterable may be invalidated as soon as the next element
	 * is requested. Thus, keeping a reference to an edge almost always leads to
	 * undesired behavior.
	 *
	 * @return the out edges (sort order determined by subclass implementation).
	 */
	public Iterable<Edge<I, E>> getEdges() {
		return edges;
	}

	/**
	 * Get an iterable of out-edges that can be modified in-place. This can mean
	 * changing the current edge value or removing the current edge (by using
	 * the iterator version). Note: if
	 *
	 * @return An iterable of mutable out-edges
	 */
	public Iterable<MutableEdge<I, E>> getMutableEdges() {
		// If the OutEdges implementation has a specialized mutable iterator,
		// we use that; otherwise, we build a new data structure as we iterate
		// over the current edges.
		if (edges instanceof MutableOutEdges) {
			return new Iterable<MutableEdge<I, E>>() {
				@Override
				public Iterator<MutableEdge<I, E>> iterator() {
					return ((MutableOutEdges<I, E>) edges).mutableIterator();
				}
			};
		} else {
			return new MutableEdgesIterable<I, E>(this);
		}
	}

	/**
	 * If a {@link MutableEdgesWrapper} was used to provide a mutable iterator,
	 * copy any remaining edges to the new
	 * {@link org.apache.giraph.edge.OutEdges} data structure and keep a direct
	 * reference to it (thus discarding the wrapper). Called by the Giraph
	 * infrastructure after computation.
	 */
	public void unwrapMutableEdges() {
		if (edges instanceof MutableEdgesWrapper) {
			edges = ((MutableEdgesWrapper<I, E>) edges).unwrap();
		}
	}

	/**
	 * Get the number of outgoing edges on this vertex.
	 *
	 * @return the total number of outbound edges from this vertex
	 */
	public int getNumEdges() {
		if (edges == null)
			return 0;
		return edges.size();
	}

	/**
	 * Return the value of the first edge with the given target vertex id, or
	 * null if there is no such edge. Note: edge value objects returned by this
	 * method may be invalidated by the next call. Thus, keeping a reference to
	 * an edge value almost always leads to undesired behavior.
	 *
	 * @param targetVertexId
	 *            Target vertex id
	 * @return Edge value (or null if missing)
	 */
	public E getEdgeValue(I targetVertexId) {
		// If the OutEdges implementation has a specialized random-access
		// method, we use that; otherwise, we scan the edges.
		if (edges instanceof StrictRandomAccessOutEdges) {
			return ((StrictRandomAccessOutEdges<I, E>) edges)
					.getEdgeValue(targetVertexId);
		} else {
			for (Edge<I, E> edge : edges) {
				if (edge.getTargetVertexId().equals(targetVertexId)) {
					return edge.getValue();
				}
			}
			return null;
		}
	}

	/**
	 * If an edge to the target vertex exists, set it to the given edge value.
	 * This only makes sense with strict graphs.
	 *
	 * @param targetVertexId
	 *            Target vertex id
	 * @param edgeValue
	 *            Edge value
	 */
	public void setEdgeValue(I targetVertexId, E edgeValue) {
		// If the OutEdges implementation has a specialized random-access
		// method, we use that; otherwise, we scan the edges.
		if (edges instanceof StrictRandomAccessOutEdges) {
			((StrictRandomAccessOutEdges<I, E>) edges).setEdgeValue(
					targetVertexId, edgeValue);
		} else {
			for (MutableEdge<I, E> edge : getMutableEdges()) {
				if (edge.getTargetVertexId().equals(targetVertexId)) {
					edge.setValue(edgeValue);
				}
			}
		}
	}

	/**
	 * Get an iterable over the values of all edges with the given target vertex
	 * id. This only makes sense for multigraphs (i.e. graphs with parallel
	 * edges). Note: edge value objects returned by this method may be
	 * invalidated as soon as the next element is requested. Thus, keeping a
	 * reference to an edge value almost always leads to undesired behavior.
	 *
	 * @param targetVertexId
	 *            Target vertex id
	 * @return Iterable of edge values
	 */
	public Iterable<E> getAllEdgeValues(final I targetVertexId) {
		// If the OutEdges implementation has a specialized random-access
		// method, we use that; otherwise, we scan the edges.
		if (edges instanceof MultiRandomAccessOutEdges) {
			return ((MultiRandomAccessOutEdges<I, E>) edges)
					.getAllEdgeValues(targetVertexId);
		} else {
			return new Iterable<E>() {
				@Override
				public Iterator<E> iterator() {
					return new UnmodifiableIterator<E>() {
						/** Iterator over all edges. */
						private Iterator<Edge<I, E>> edgeIterator = edges
								.iterator();
						/** Last matching edge found. */
						private Edge<I, E> currentEdge;

						@Override
						public boolean hasNext() {
							while (edgeIterator.hasNext()) {
								currentEdge = edgeIterator.next();
								if (currentEdge.getTargetVertexId().equals(
										targetVertexId)) {
									return true;
								}
							}
							return false;
						}

						@Override
						public E next() {
							return currentEdge.getValue();
						}
					};
				}
			};
		}
	}

	/**
	 * Send a message to a vertex id. The message should not be mutated after
	 * this method returns or else undefined results could occur.
	 *
	 * @param id
	 *            Vertex id to send the message to
	 * @param message
	 *            Message data to send. Note that after the message is sent, the
	 *            user should not modify the object.
	 */
	public void sendMessage(I id, M message) {
		if (graphState.getWorkerClientRequestProcessor().sendMessageRequest(id,
				message)) {
			graphState.getGraphTaskManager().notifySentMessages();
		}
	}

	/**
	 * Send message using edge information. Used only by ECEdgePartition
	 * 
	 * @param edge
	 */
	public void sendMessageViaEdge(Edge<I, E> edge) {

	}

	/**
	 * Send a message to all edges.
	 *
	 * @param message
	 *            Message sent to all edges.
	 */
	public void sendMessageToAllEdges(M message) {
		if (edges == null)
			return;
		for (Edge<I, E> edge : getEdges()) {
			sendMessage(edge.getTargetVertexId(), message);
		}
	}

	public void broadcastValueToAllWorkers(M message) {
		if (graphState.getWorkerClientRequestProcessor().broadcastValueRequest(
				id, message)) {
			graphState.getGraphTaskManager().notifySentMessages();
		}
	}

	/**
	 * After this is called, the compute() code will no longer be called for
	 * this vertex unless a message is sent to it. Then the compute() code will
	 * be called once again until this function is called. The application
	 * finishes only when all vertices vote to halt.
	 */
	public void voteToHalt() {
		halt = true;
	}

	/**
	 * Re-activate vertex if halted.
	 */
	public void wakeUp() {
		halt = false;
	}

	/**
	 * Is this vertex done?
	 *
	 * @return True if halted, false otherwise.
	 */
	public boolean isHalted() {
		return halt;
	}

	/**
	 * Add an edge for this vertex (happens immediately)
	 *
	 * @param edge
	 *            Edge to add
	 */
	public void addEdge(Edge<I, E> edge) {
		edges.add(edge);
	}

	/**
	 * Removes all edges pointing to the given vertex id.
	 *
	 * @param targetVertexId
	 *            the target vertex id
	 */
	public void removeEdges(I targetVertexId) {
		edges.remove(targetVertexId);
	}

	/**
	 * Sends a request to create a vertex that will be available during the next
	 * superstep.
	 *
	 * @param id
	 *            Vertex id
	 * @param value
	 *            Vertex value
	 * @param edges
	 *            Initial edges
	 */
	public void addVertexRequest(I id, V value, OutEdges<I, E> edges)
			throws IOException {
		Vertex<I, V, E, M> vertex = getConf().createVertex();
		vertex.initialize(id, value, edges);
		graphState.getWorkerClientRequestProcessor().addVertexRequest(vertex);
	}

	/**
	 * Sends a request to create a vertex that will be available during the next
	 * superstep.
	 *
	 * @param id
	 *            Vertex id
	 * @param value
	 *            Vertex value
	 */
	public void addVertexRequest(I id, V value) throws IOException {
		addVertexRequest(id, value, getConf().createAndInitializeOutEdges());
	}

	/**
	 * Request to remove a vertex from the graph (applied just prior to the next
	 * superstep).
	 *
	 * @param vertexId
	 *            Id of the vertex to be removed.
	 */
	public void removeVertexRequest(I vertexId) throws IOException {
		graphState.getWorkerClientRequestProcessor().removeVertexRequest(
				vertexId);
	}

	/**
	 * Request to add an edge of a vertex in the graph (processed just prior to
	 * the next superstep)
	 *
	 * @param sourceVertexId
	 *            Source vertex id of edge
	 * @param edge
	 *            Edge to add
	 */
	public void addEdgeRequest(I sourceVertexId, Edge<I, E> edge)
			throws IOException {
		graphState.getWorkerClientRequestProcessor().addEdgeRequest(
				sourceVertexId, edge);
	}

	/**
	 * Request to remove all edges from a given source vertex to a given target
	 * vertex (processed just prior to the next superstep).
	 *
	 * @param sourceVertexId
	 *            Source vertex id
	 * @param targetVertexId
	 *            Target vertex id
	 */
	public void removeEdgesRequest(I sourceVertexId, I targetVertexId)
			throws IOException {
		graphState.getWorkerClientRequestProcessor().removeEdgesRequest(
				sourceVertexId, targetVertexId);
	}

	/**
	 * Set the graph state for all workers
	 *
	 * @param graphState
	 *            Graph state for all workers
	 */
	public void setGraphState(GraphState<I, V, E, M> graphState) {
		this.graphState = graphState;
	}

	/**
	 * Get the mapper context
	 *
	 * @return Mapper context
	 */
	public Mapper.Context getContext() {
		return graphState.getContext();
	}

	/**
	 * Get the partition context
	 *
	 * @return Partition context
	 */
	public PartitionContext getPartitionContext() {
		return graphState.getPartitionContext();
	}

	/**
	 * Get the worker context
	 *
	 * @return WorkerContext context
	 */
	public WorkerContext getWorkerContext() {
		return graphState.getGraphTaskManager().getWorkerContext();
	}

	@Override
	public <A extends Writable> void aggregate(String name, A value) {
		graphState.getWorkerAggregatorUsage().aggregate(name, value);
	}

	@Override
	public <A extends Writable> A getAggregatedValue(String name) {
		return graphState.getWorkerAggregatorUsage().<A> getAggregatedValue(
				name);
	}

	@Override
	public String toString() {
		return "Vertex(id=" + getId() + ",value=" + getValue() + ",#edges="
				+ getNumEdges() + ")";
	}
}
