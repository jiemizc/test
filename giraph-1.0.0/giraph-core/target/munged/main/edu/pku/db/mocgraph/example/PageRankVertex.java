package edu.pku.db.mocgraph.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import edu.pku.db.mocgraph.example.MultipleLandmarkVertex.MultipleLandmarkContext;

public class PageRankVertex extends Vertex<IntWritable, DoubleWritable, NullWritable, DoubleWritable>{
	public static String SUPERSTEP_COUNT = "superstep.max";
	public static int total_superstep = -1;
	
	public void computeSingleMsg(DoubleWritable value, DoubleWritable message, long superstep) {
		value.set(value.get() + message.get());
	}

	public void sendMessages(DoubleWritable value, long superstep) {
		DoubleWritable msg = new DoubleWritable();
		msg.set((0.85f * (value.get()) + 0.15f / getTotalNumVertices())
				/ getNumEdges());
		if (superstep < total_superstep) {
			value.set(0);
			sendMessageToAllEdges(msg);
		} else {
			double newv = 0.85f * value.get() + 0.15f / getTotalNumVertices();
			value.set(newv);
			voteToHalt();
		}
	}
	
	@Override
	public void compute(Iterable<DoubleWritable> messages) throws IOException {
		double sum = 0;   
		if (getSuperstep() >= 1) {
	      for (DoubleWritable message : messages) {
	        sum += message.get();
	      }
	      sum = (0.15f / getTotalNumVertices()) + 0.85f * sum;
	      getValue().set(sum);
	    }

	    if (getSuperstep() < total_superstep) {
	      long edges = getNumEdges();
	      DoubleWritable msg = new DoubleWritable(sum/edges);
	      sendMessageToAllEdges(msg);
	    } else {
	      voteToHalt();
	    }
    }
	
	public static class PageRankContext extends WorkerContext {

		@Override
		public void preApplication() {
			total_superstep = getContext().getConfiguration().getInt(SUPERSTEP_COUNT, 5);
		}

		@Override
		public void postApplication() {
		}

		@Override
		public void preSuperstep() {
		}

		@Override
		public void postSuperstep() {
		}

	}
	
	@Override
	public DoubleWritable createNewValue(DoubleWritable curValue, long superstep){
		if (superstep < total_superstep) {
			return new DoubleWritable(0);
		} else {
			return curValue;
		}
	}
}
