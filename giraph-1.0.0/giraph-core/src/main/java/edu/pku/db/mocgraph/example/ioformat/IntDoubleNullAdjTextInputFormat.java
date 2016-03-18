package edu.pku.db.mocgraph.example.ioformat;

import java.io.IOException;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;

public class IntDoubleNullAdjTextInputFormat
		extends
		TextVertexInputFormat<IntWritable, DoubleWritable, NullWritable> {

	@Override
	public TextVertexReaderFromEachLineProcessed<String[]> createVertexReader(InputSplit split,
			TaskAttemptContext context) {
		return new IntNullNullAdjacencyListVertexReader();
	}

	/**
	 * VertexReader associated with
	 * {@link IntDoubleDoubleAdjacencyListVertexInputFormat}.
	 */
	protected class IntNullNullAdjacencyListVertexReader extends
			TextVertexReaderFromEachLineProcessed<String[]> {

		@Override
		protected String[] preprocessLine(Text line) throws IOException {
			String[] splits = line.toString().split("\t");
			return splits;
		}

		@Override
		protected IntWritable getId(String[] line) throws IOException {
			return new IntWritable(Integer.parseInt(line[0]));
		}

		@Override
		protected DoubleWritable getValue(String[] line) throws IOException {
			return new DoubleWritable(0);
		}

		@Override
		protected Iterable<Edge<IntWritable, NullWritable>> getEdges(
				String[] line) throws IOException {
			int i = 1;
			List<Edge<IntWritable, NullWritable>> edges = Lists.newLinkedList();
			while (i < line.length) {
				edges.add(EdgeFactory.create(new IntWritable(Integer.parseInt(line[i])),
						NullWritable.get()));
				i++;
			}
			return edges;
		}

	}

}
