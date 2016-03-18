package edu.pku.db.mocgraph.example;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.aggregators.TextAggregatorWriter;
import org.apache.giraph.edge.HashMapEdges;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.IntIntNullTextInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.pku.db.mocgraph.example.ioformat.IntNullNullAdjTextInputFormat;

/**
 * Default Pregel-style PageRank computation.
 */
public class TriangleCounting implements Tool {
	/** Class logger */
	private static final Logger LOG = Logger.getLogger(TriangleCounting.class);
	/** Configuration from Configurable */
	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public final int run(final String[] args) throws Exception {
		Options options = new Options();
		options.addOption("h", "help", false, "help");
		options.addOption("v", "verbose", false, "whether run in verbose");
		options.addOption("indir", "inpath", true, "data-inputpath");
		options.addOption("outdir", "outpath", true, "result-outputpath");
		options.addOption("w", "workers", true, "Number of workers");
		options.addOption("N", "name", true, "Name of the job");

		// ///////////////////////////////////////////////////////////
		options.addOption("tn", "thread number", true,
				"thread number per worker");
		options.addOption("rg", "run with Giraph", false,
				"run with Giraph implementation, default runs with MOCgraph");
		options.addOption("pn", "partitionNum", true,
				"partition number per worker");
		options.addOption("ooc", "outOfCore", false, "run in outofcore mode");
		options.addOption(
				"nimp",
				"numOfInMemoryPartition",
				true,
				"specify the maximum partition number that can be in memory. default is half of the $pn");
		options.addOption("imp", "inMemoryPercentage", true,
				"maximum percentage of the in memory message size w.r.t the maximum jvm mem");
		// ///////////////////////////////////////////////////////////
		
		HelpFormatter formatter = new HelpFormatter();
		if (args.length == 0) {
			formatter.printHelp(getClass().getName(), options, true);
			return 0;
		}
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

		String name = getClass().getName();
		if (cmd.hasOption("N")) {
			name = cmd.getOptionValue("N");
		}
		GiraphJob job = new GiraphJob(getConf(), name);
		job.getConfiguration().setVertexClass(TriangleCountingVertex.class);

		if (cmd.hasOption('h')) {
			formatter.printHelp(getClass().getName(), options, true);
			return 0;
		}
		if (!cmd.hasOption("indir")) {
			LOG.info("Need to specify the data path (-indir)");
			return -1;
		}
		if (!cmd.hasOption("outdir")) {
			LOG.info("Need to specify the data path (-outdir)");
			return -1;
		}
		if (!cmd.hasOption('w')) {
			LOG.info("Need to choose the number of workers (-w)");
			return -1;
		} else {
			int workers = Integer.parseInt(cmd.getOptionValue('w'));
			job.getConfiguration().setWorkerConfiguration(workers, workers,
					100f);
		}

		// ///////////////////////////////////////////////////////////
		// whether use original giraph or mocgraph
		if (cmd.hasOption("rg"))
			job.getConfiguration().setUseOnlineCompute(false);
		else
			job.getConfiguration().setUseOnlineCompute(true);
		
		// compute thread number
		if (cmd.hasOption("pn")) {
			job.getConfiguration().setInt(
					"giraph.userPartitionCount",
					Integer.parseInt(cmd.getOptionValue('w'))
							* Integer.parseInt(cmd.getOptionValue("pn")));
		}

		// out-of-core mode
		if (cmd.hasOption("ooc")) {
			job.getConfiguration().setBoolean("giraph.useOutOfCoreGraph", true);
			job.getConfiguration().setBoolean("giraph.useOutOfCoreMessages",
					true);
			if (cmd.hasOption("nimp"))
				job.getConfiguration().setInt("giraph.maxPartitionsInMemory",
						Integer.parseInt(cmd.getOptionValue("nimp")));
			else
				job.getConfiguration()
						.setInt("giraph.maxPartitionsInMemory",
								(int) (Math.max(1, job.getConfiguration().getInt(
										"giraph.userPartitionCount", 10) * 0.5 / Integer
										.parseInt(cmd.getOptionValue('w')))));
			if (cmd.hasOption("imp"))
				job.getConfiguration().setFloat("giraph.messageBuffer.percent",
						Float.parseFloat(cmd.getOptionValue("imp")));
		} else
			job.getConfiguration()
					.setBoolean("giraph.useOutOfCoreGraph", false);

		if (cmd.hasOption("tn")) {
			int tn = Integer.parseInt(cmd.getOptionValue("pn"));
			job.getConfiguration().setNumComputeThreads(tn);
			if (tn > 1) {
				job.getConfiguration().setBoolean(
						"giraph.useThreadLocalAggregators", true);
			}
		}

		job.getConfiguration().setOutEdgesClass(HashMapEdges.class);
		job.getConfiguration().setBoolean("giraph.isStaticGraph", true);

		// job.getConfiguration().setBoolean("giraph.SplitMasterWorker", false);
		// job.getConfiguration().setZooKeeperConfiguration("localhost:2181");
		// job.getConfiguration().setWorkerConfiguration(1, 1, 100.0f);

		// job.getConfiguration().setBoolean("giraph.useOutOfCoreMessages",
		// true);
		// job.getConfiguration().setBoolean("giraph.useOutOfCoreGraph", true);
		// job.getConfiguration().setInt("giraph.maxPartitionsInMemory", 10);
		// job.getConfiguration().setFloat("giraph.messageBuffer.percent",
		// 0.2f);
		// job.getConfiguration().setInt("giraph.maxMessagesInMemory", 100000);

		// job.getConfiguration().setLong("giraph.vertexKeySpaceSize",
		// 42000000);
		// job.getConfiguration().setGraphPartitionerFactoryClass(SimpleIntRangePartitionerFactory.class);

		job.getConfiguration().setVertexInputFormatClass(
				IntNullNullAdjTextInputFormat.class);

		// job.getConfiguration().set("mapred.child.java.opts",
		// "-Xms256m -Xmx8g -XX:+UseSerialGC");
		// job.getConfiguration().set("mapred.job.map.memory.mb", "8000");
		// job.getConfiguration().set("hadoop.job.ugi", "zhouchang,supergroup");

		Path outputPath = new Path(cmd.getOptionValue("outdir"));
		GiraphFileInputFormat.addVertexInputPath(job.getConfiguration(),
				new Path(cmd.getOptionValue("indir")));
		FileOutputFormat.setOutputPath(job.getInternalJob(), outputPath);
		job.getConfiguration().setInt("giraph.textAggregatorWriter.frequency",
				1);
		job.getConfiguration().setAggregatorWriterClass(
				TextAggregatorWriter.class);
		job.getConfiguration().set(TextAggregatorWriter.FILENAME,
				cmd.getOptionValue("outdir") + "/aggregatorPath");
		job.getConfiguration().setMasterComputeClass(
				TriangleMasterCompute.class);

		// job.getConfiguration().setVertexOutputFormatClass(
		// IdWithValueTextOutputFormat.class);

		boolean isVerbose = false;
		FileSystem fs = FileSystem.get(job.getConfiguration());
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		if (cmd.hasOption('v')) {
			isVerbose = true;
		}
		if (job.run(isVerbose)) {
			return 0;
		} else {
			return -1;
		}
	}

	public static class TriangleMasterCompute extends DefaultMasterCompute {
		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {
			registerAggregator("triangleNum", LongSumAggregator.class);
		}
	}

	/**
	 * Execute the benchmark.
	 *
	 * @param args
	 *            Typically the command line arguments.
	 * @throws Exception
	 *             Any exception from the computation.
	 */
	public static void main(final String[] args) throws Exception {
		System.exit(ToolRunner.run(new TriangleCounting(), args));
	}
}
