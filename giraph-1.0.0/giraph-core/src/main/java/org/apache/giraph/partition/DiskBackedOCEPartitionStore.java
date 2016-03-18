///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.giraph.partition;
//
//import static org.apache.giraph.conf.GiraphConstants.MAX_PARTITIONS_IN_MEMORY;
//import static org.apache.giraph.conf.GiraphConstants.PARTITIONS_DIRECTORY;
//
//import java.io.BufferedInputStream;
//import java.io.BufferedOutputStream;
//import java.io.DataInput;
//import java.io.DataInputStream;
//import java.io.DataOutput;
//import java.io.DataOutputStream;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//import java.util.Set;
//import java.util.concurrent.AbstractExecutorService;
//import java.util.concurrent.Callable;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.locks.Condition;
//import java.util.concurrent.locks.Lock;
//import java.util.concurrent.locks.ReentrantReadWriteLock;
//
//import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
//import org.apache.giraph.edge.OutEdges;
//import org.apache.giraph.graph.Vertex;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.io.WritableComparable;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
//import org.apache.log4j.Logger;
//
//import com.google.common.collect.Iterables;
//import com.google.common.collect.MapMaker;
//import com.google.common.collect.Maps;
//import com.google.common.collect.Sets;
//import com.google.common.hash.HashFunction;
//import com.google.common.hash.Hashing;
//
///**
// * Disk-backed PartitionStore. 
// * Partitions are stored by vertexStore and edgeStore respectively.
// * Hot partitions resides in memory permanently, 
// * while only one partition is for cold partition swap in and out.
// * 
// * Thread-safe, but expects the caller to synchronized between deletes, adds,
// * puts and gets.
// * @param <I>
// *            Vertex id
// * @param <V>
// *            Vertex data
// * @param <E>
// *            Edge data
// * @param <M>
// *            Message data
// */
//@SuppressWarnings("rawtypes")
//public class DiskBackedOCEPartitionStore<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable>
//		extends PartitionStore<I, V, E, M> {
//	/** Class logger. */
//	private static final Logger LOG = Logger
//			.getLogger(DiskBackedOCEPartitionStore.class);
//	ConcurrentHashMap<Integer, Partition<I,V,E,M>> initialPartitions = new ConcurrentHashMap<Integer, Partition<I,V,E,M>>();
//	boolean transferedToHot = false;
//	Map<Integer, Partition<I,V,E,M>>  hotPartitions = new HashMap<Integer, Partition<I,V,E,M>>();
//	Partition<I,V,E,M> swapPartition;
//	@Override
//	public void addPartition(Partition<I, V, E, M> partition) {
//		Partition<I, V, E, M> rawP = initialPartitions.get(partition.getId());
//		if(rawP==null){
//			Partition<I, V, E, M> insertedP = 
//					initialPartitions.putIfAbsent(partition.getId(), partition);
//			rawP = insertedP;
//		}
//		if(rawP!=null){
//			rawP.addPartition(partition);
//		}
//	}
//
//	@Override
//	public Partition<I, V, E, M> getPartition(Integer partitionId) {
//		
//		return null;
//	}
//
//	ConcurrentHashMap<Integer, Partition<I, V, E, M>> edgeCountMap;
//	public void hotAnalysis() {
//		List<Integer> sortedPartitionOnHot = new ArrayList<Integer>(edgeCountMap.keySet());
//		Collections.sort(sortedPartitionOnHot, new Comparator<Integer>() {
//
//			@Override
//			public int compare(Integer o1, Integer o2) {
//				Long hot1 = hotMap.get(o1);
//				Long hot2 = hotMap.get(o2);
//				if (hot1 > hot2)
//					return -1;
//				else if (hot1 == hot2)
//					return 0;
//				else
//					return 1;
//			}
//
//		});
//		// hot partition are all reside in memory and do not swap to disk.
//		int hotPartitionNum = maxInMemoryPartitions - 1;
//		hotPartitions = new HashSet<Integer>();
//
//		for (int i = 0; i < hotPartitionNum; i++)
//			hotPartitions.add(sortedPartitionOnHot.get(i));
//
//		// copy inactive partitions to inactiveColdPartitions if it's cold.
//		inactiveColdPartitions = Maps.newLinkedHashMap();
//		for (Entry<Integer, Partition<I, V, E, M>> entry : inactive.entrySet()) {
//			if (!hotPartitions.contains(entry.getKey())) {
//				inactiveColdPartitions.put(entry.getKey(), entry.getValue());
//			}
//			;
//		}
//		// cold partition would be swap to disk when memory is not enough.
//		int coldPartitionNum = partitionIds.size() - hotPartitionNum;
//		StringBuffer hotP = new StringBuffer();
//		for (Integer id : hotPartitions) {
//			long hotValue = this.hotMap.get(id);
//			hotP.append("(" + id + ", " + hotValue + ")|");
//		}
//		LOG.info("HOTS analysis: HOT partition HOTS:" + hotP.toString());
//		hotP = new StringBuffer();
//		for (Integer id : sortedPartitionOnHot) {
//			hotP.append("(" + id + ", " + hotMap.get(id) + ")|");
//		}
//		LOG.info("HOTS analysis: ALL partition HOTS:" + hotP.toString());
//		LOG.info("now adjust memory to fill with hot partitions...");
//		for (Integer id : hotPartitions) {
//			Partition<I, V, E, M> p = getPartitionIfInMemory(id);
//			if(p==null)
//				p = getPartition(id);
//			putPartition(p, 1);
//		}
//	}
//	@Override
//	public void putPartition(Partition<I, V, E, M> partition) {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public Partition<I, V, E, M> removePartition(Integer partitionId) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public void deletePartition(Integer partitionId) {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public boolean hasPartition(Integer partitionId) {
//		// TODO Auto-generated method stub
//		return false;
//	}
//
//	@Override
//	public Iterable<Integer> getPartitionIds() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public int getNumPartitions() {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//}
