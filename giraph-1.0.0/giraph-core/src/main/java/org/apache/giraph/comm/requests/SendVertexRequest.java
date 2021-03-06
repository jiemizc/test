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

package org.apache.giraph.comm.requests;

import org.apache.giraph.comm.ServerData;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import com.google.common.collect.Lists;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

/**
 * Send a collection of vertices for a partition.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class SendVertexRequest<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable> extends
    WritableRequest<I, V, E, M> implements WorkerRequest<I, V, E, M> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SendVertexRequest.class);
  /** Partition */
  private Partition<I, V, E, M> partition;

  /**
   * Constructor used for reflection only
   */
  public SendVertexRequest() { }

  /**
   * Constructor for sending a request.
   *
   * @param partition Partition to send the request to
   */
  public SendVertexRequest(Partition<I, V, E, M> partition) {
    this.partition = partition;
  }

  @Override
  public void readFieldsRequest(DataInput input) throws IOException {
    partition = getConf().createPartition(-1, null);
    partition.readFields(input);
  }

  @Override
  public void writeRequest(DataOutput output) throws IOException {
    partition.write(output);
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_VERTEX_REQUEST;
  }

  @Override
  public void doRequest(ServerData<I, V, E, M> serverData) {
	serverData.getPartitionStore().addPartition(partition);
  }

  @Override
  public int getSerializedSize() {
    return WritableRequest.UNKNOWN_SIZE;
  }
}

