package edu.pku.db.mocgraph.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.giraph.io.formats.AdjacencyListTextVertexOutputFormat;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.pku.db.mocgraph.example.MultipleLandmarkVertex.MultipleLandmarkContext;
import edu.pku.db.mocgraph.example.ioformat.IntLandmarkTableIntAdjTextInputFormat;
import edu.pku.db.mocgraph.example.ioformat.MOCgraphIdWithValueTextOutputFormat;

/**
 * Default Pregel-style PageRank computation.
 */
public class Landmark implements Tool {
	/** Class logger */
	private static final Logger LOG = Logger.getLogger(Landmark.class);
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
		options.addOption("s", "dubug", true, "debug");
		options.addOption("indir", "inpath", true, "data-path");
		options.addOption("outdir", "outpath", true, "data-path");
		options.addOption("w", "workers", true, "Number of workers");
		options.addOption("N", "name", true, "Name of the job");
		options.addOption("markdir", "landmark point path", true,
				"landmark point path");

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
		options.addOption("es", "edgeSeparation", false, "use edge separation for out-of-core engine");
		// ///////////////////////////////////////////////////////////

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
		if (!cmd.hasOption("markdir")) {
			LOG.info("Need to specify the markdir");
			return -1;
		}

		String name = getClass().getName();
		if (cmd.hasOption('N')) {
			name = name + " " + cmd.getOptionValue('N');
		}

		GiraphJob job = new GiraphJob(getConf(), name);
		job.getConfiguration().setBoolean("giraph.isStaticGraph", true);
		job.getConfiguration().setWorkerContextClass(
				MultipleLandmarkContext.class);
		job.getConfiguration().setVertexClass(MultipleLandmarkVertex.class);

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
			if(cmd.hasOption("es"))
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
		String path = cmd.getOptionValue("markdir");
		job.getConfiguration().set("markdir", path);
		// ///////////////////////////////////////////////////////

		job.getConfiguration().setVertexInputFormatClass(
				IntLandmarkTableIntAdjTextInputFormat.class);
		job.getConfiguration().setVertexOutputFormatClass(
				MOCgraphIdWithValueTextOutputFormat.class);
		int workers = Integer.parseInt(cmd.getOptionValue('w'));
		job.getConfiguration().setWorkerConfiguration(workers, workers, 100f);

		GiraphFileInputFormat.addVertexInputPath(job.getConfiguration(),
				new Path(cmd.getOptionValue("indir")));
		FileOutputFormat.setOutputPath(job.getInternalJob(),
				new Path(cmd.getOptionValue("outdir")));
		// job.getConfiguration().setBoolean("giraph.SplitMasterWorker", false);
		// job.getConfiguration().setZooKeeperConfiguration("localhost:2181");
		// job.getConfiguration().setWorkerConfiguration(1, 1, 100.0f);

		// job.getConfiguration().setBoolean("giraph.vertexOnlyForHot", true);

		FileSystem fs = FileSystem.get(job.getConfiguration());
		if (fs.exists(new Path(cmd.getOptionValue("outdir")))) {
			fs.delete(new Path(cmd.getOptionValue("outdir")), true);
		}
		boolean isVerbose = false;
		if (cmd.hasOption('v')) {
			isVerbose = true;
		}
		if (job.run(isVerbose)) {
			return 0;
		} else {
			return -1;
		}
	}

	public List<Integer> getLandmarks(String uri) {
		List<Integer> landmarks = new ArrayList<Integer>();
		BufferedReader br = null;
		try {
			FileSystem fs = FileSystem
					.get(URI.create(uri), new Configuration());
			Path path = new Path(uri);

			String line;
			br = new BufferedReader(new InputStreamReader(fs.open(path)));
			while ((line = br.readLine()) != null) {
				Integer id = Integer.parseInt(line);
				landmarks.add(id);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return landmarks;
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
		// (new LandmarkBenchmark())
		// .getLandmarks("/user/zhouchang/queryPatterns/00_1.0_9999_50_1.txt");
		System.exit(ToolRunner.run(new Landmark(), args));
	}
}
