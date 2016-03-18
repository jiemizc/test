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

import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Progressable;
import org.mortbay.log.Log;

import com.google.common.collect.Maps;

import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.apache.giraph.conf.GiraphConstants.USE_OUT_OF_CORE_MESSAGES;

/**
 * A simple map-based container that stores vertices.  Vertex ids will map to
 * exactly one partition.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class SimplePartition<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends BasicPartition<I, V, E, M> {
  /** Vertex map for this range (keyed by index) */
  private ConcurrentMap<I, Vertex<I, V, E, M>> vertexMap;
  private long edgeCount = 0;
  /**
   * Constructor for reflection.
   */
  public SimplePartition() { }

  @Override
  public void initialize(int partitionId, Progressable progressable) {
    super.initialize(partitionId, progressable);
    if (USE_OUT_OF_CORE_MESSAGES.get(getConf())) {
      vertexMap = new ConcurrentSkipListMap<I, Vertex<I, V, E, M>>();
    } else {
      vertexMap = Maps.newConcurrentMap();
    }
  }

  @Override
  public Vertex<I, V, E, M> getVertex(I vertexIndex) {
    return vertexMap.get(vertexIndex);
  }

  @Override
  public Vertex<I, V, E, M> putVertex(Vertex<I, V, E, M> vertex) {
	Vertex<I,V,E,M> ret = vertexMap.put(vertex.getId(), vertex);
	if(ret==null) 
		edgeCount += vertex.getNumEdges();
    return ret;
  }

  @Override
  public Vertex<I, V, E, M> removeVertex(I vertexIndex) {
    Vertex<I,V,E,M> ret = vertexMap.remove(vertexIndex);
    if(ret!=null)
    	edgeCount -= ret.getTotalNumEdges();
    return ret;
  }

  @Override
  public void addPartition(Partition<I, V, E, M> partition) {
    for (Vertex<I, V, E , M> vertex : partition) {
      putVertex(vertex);
    }
  }
  
  public void addPartition(Partition<I, V, E, M> partition, String edgeFilePath){
	  try {
		  File file = new File(edgeFilePath);
		  if(!file.exists()){
			  file.getParentFile().mkdirs();
			  file.createNewFile();
		  }
		  DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(
					new FileOutputStream(file, true)));
		  for (Vertex<I, V, E, M> vertex : partition) {
			  Vertex<I,V,E,M> ret = vertexMap.put(vertex.getId(), vertex);
			  edgeCount += vertex.getNumEdges();
			  vertex.getId().write(outputStream);
			  ((OutEdges<I, E>) vertex.getEdges()).write(outputStream);
//			  vertex.setEdges(getConf().createAndInitializeInputOutEdges());
			  vertex.setEdges(null);
		  }
		  outputStream.close();
	  } catch (Exception e) {
			e.printStackTrace();
      }
  }

  @Override
  public long getVertexCount() {
    return vertexMap.size();
  }

  @Override
  public long getEdgeCount() {
    return edgeCount;
  }

  @Override
  public void saveVertex(Vertex<I, V, E, M> vertex) {
    // No-op, vertices are stored as Java objects in this partition
  }

  @Override
  public String toString() {
    return "(id=" + getId() + ",V=" + vertexMap.size() +  ", E=" + getEdgeCount()+")";
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    super.readFields(input);
    if (USE_OUT_OF_CORE_MESSAGES.get(getConf())) {
      vertexMap = new ConcurrentSkipListMap<I, Vertex<I, V, E, M>>();
    } else {
      vertexMap = Maps.newConcurrentMap();
    }
    int vertices = input.readInt();
    edgeCount = input.readLong();
    for (int i = 0; i < vertices; ++i) {
      progress();
      Vertex<I, V, E, M> vertex =
          WritableUtils.readVertexFromDataInput(input, getConf());
      if (vertexMap.put(vertex.getId(), vertex) != null) {
        throw new IllegalStateException(
            "readFields: " + this +
            " already has same id " + vertex);
      }
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    super.write(output);
    output.writeInt(vertexMap.size());
    output.writeLong(edgeCount);
    for (Vertex<I, V, E, M> vertex : vertexMap.values()) {
      progress();
      WritableUtils.writeVertexToDataOutput(output, vertex, getConf());
    }
  }

  @Override
  public Iterator<Vertex<I, V, E, M>> iterator() {
    return vertexMap.values().iterator();
  }
}
