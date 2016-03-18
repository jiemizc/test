package edu.pku.db.mocgraph.example;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.pku.db.mocgraph.example.PageRankVertex.PageRankContext;
import edu.pku.db.mocgraph.example.ioformat.IntDoubleNullAdjTextInputFormat;
import edu.pku.db.mocgraph.example.ioformat.MOCgraphIdWithValueTextOutputFormat;

/**
 * Default Pregel-style PageRank computation.
 */
public class PageRank implements Tool {
	/** Class logger */
	private static final Logger LOG = Logger.getLogger(PageRank.class);
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
		options.addOption("h", "help", false, "Help");
		options.addOption("v", "verbose", false, "Verbose");
		options.addOption("indir", "inpath", true, "data-path");
		options.addOption("outdir", "outpath", true, "data-path");
		options.addOption("s", "superstep.max", true, "superstep.max");
		options.addOption("w", "workers", true, "Number of workers");
		options.addOption("N", "name", true, "Name of the job");
		// ///////////////////////////////////////////////////////////
		options.addOption("sync", "runInSyncMode", false,
				"run in synchronous mode with dual version switch");
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
		options.addOption("es", "edgeSeparation", false,
				"use edge separation for out-of-core engine");
		// /////////////////////////////////////////////////////////////////

		HelpFormatter formatter = new HelpFormatter();
		if (args.length == 0) {
			formatter.printHelp(getClass().getName(), options, true);
			return 0;
		}
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

		if (cmd.hasOption('h')) {
			formatter.printHelp(getClass().getName(), options, true);
			return 0;
		}
		if (!cmd.hasOption("s")) {
			LOG.info("Need to specify the superstep max round");
			return -1;
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
		}

		int workers = Integer.parseInt(cmd.getOptionValue('w'));
		String name = getClass().getName();
		if (cmd.hasOption("N")) {
			name = " " + cmd.getOptionValue("N");
		}

		GiraphJob job = new GiraphJob(getConf(), name);

		// ///////////////////////////////////////////////////////////
		// whether use original giraph or mocgraph
		if (cmd.hasOption("sync"))
			job.getConfiguration().setSynchronousMode(true);
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
				job.getConfiguration().setInt(
						"giraph.maxPartitionsInMemory",
						(int) (Math.max(
								1,
								job.getConfiguration().getInt(
										"giraph.userPartitionCount", 10)
										* 0.5
										/ Integer.parseInt(cmd
												.getOptionValue('w')))));
			if (cmd.hasOption("imp"))
				job.getConfiguration().setFloat("giraph.messageBuffer.percent",
						Float.parseFloat(cmd.getOptionValue("imp")));
			if (cmd.hasOption("es"))
				job.getConfiguration().setUseEdgeSeparation(true);
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
		job.getConfiguration().setBoolean("giraph.isStaticGraph", true);

		// job.getConfiguration().setBoolean("giraph.vertexOnlyForHot", true);
		job.getConfiguration().setVertexClass(PageRankVertex.class);
		job.getConfiguration().setWorkerContextClass(PageRankContext.class);
		// job.getConfiguration().setCombinerClass(PRCombiner.class);
		job.getConfiguration().setVertexInputFormatClass(
				IntDoubleNullAdjTextInputFormat.class);
		// IntDoubleNullTextInputFormat.class);
		job.getConfiguration().setVertexOutputFormatClass(
				MOCgraphIdWithValueTextOutputFormat.class);
		GiraphFileInputFormat.addVertexInputPath(job.getConfiguration(),
				new Path(cmd.getOptionValue("indir")));
		FileOutputFormat.setOutputPath(job.getInternalJob(),
				new Path(cmd.getOptionValue("outdir")));
		job.getConfiguration().setWorkerConfiguration(workers, workers, 100.0f);
		job.getConfiguration().setLong(PageRankVertex.SUPERSTEP_COUNT,
				Long.parseLong(cmd.getOptionValue("s")));

		boolean isVerbose = false;
		if (cmd.hasOption('v')) {
			isVerbose = true;
		}
		FileSystem fs = FileSystem.get(job.getConfiguration());
		if (fs.exists(new Path(cmd.getOptionValue("outdir")))) {
			fs.delete(new Path(cmd.getOptionValue("outdir")), true);
		}
		if (job.run(isVerbose)) {
			return 0;
		} else {
			return -1;
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
		System.exit(ToolRunner.run(new PageRank(), args));
	}
}
