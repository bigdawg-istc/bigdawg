package istc.bigdawg.accumulo;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;

import org.apache.accumulo.core.cli.MapReduceClientOnRequiredTable;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.partition.RangePartitioner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Base64;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.Parameter;

/**
 * Example of a map reduce job that bulk ingests data into an Accumulo table. The
 * expected input is text files containing tab separated key value pairs on each
 * line.
 */
public class BulkLoad extends Configured implements Tool {
	public static class MapClass
			extends Mapper<LongWritable, Text, Text, Text> {
		private int[] keyColumns;
		// private byte[] a = new byte[1];
		// private int index = 0;

		protected void setup(Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String[] kc = conf.get("totem.migration.keycolumn", "1").split(":");
			keyColumns = new int[kc.length];
			for (int i = 0; i < keyColumns.length; i++) {
				keyColumns[i] = Integer.parseInt(kc[i]);
			}
		}

		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			// output.write(new Text(String.format("%1d", index)), value);
			// index++;

			Text outputKey = new Text();
			Text outputValue = new Text();
			StringBuffer keyBuf = new StringBuffer();
			StringBuffer valBuf = new StringBuffer();
			int keyCount = 0;
			int colCount = 0;
			int begin = 0;
			boolean first = true;
			for (int i = 0; i < value.getLength(); i++) {
				if (value.getBytes()[i] == '|') { // get key
					colCount++;
					if (colCount == keyColumns[keyCount]) {
						keyBuf.append(
								new String(value.getBytes(), begin, i - begin));
						begin = i;
						keyCount++;
					} else {
						if (first) {
							first = false;
							valBuf.append(new String(value.getBytes(),
									begin + 1, i - begin - 1));
						} else {
							valBuf.append(new String(value.getBytes(), begin,
									i - begin));
						}
						begin = i;
					}
					if (keyCount == keyColumns.length) {
						break;
					}
				}
			}

			if (begin < value.getLength()) {
				// System.out.println("Not Here");
				valBuf.append(new String(value.getBytes(), begin,
						value.getLength() - begin));
				outputKey.set(keyBuf.toString());
				outputValue.set(valBuf.toString());
				output.write(outputKey, outputValue);
				// value.set(a);
			}
		}
	}

	public static class ReduceClass extends Reducer<Text, Text, Key, Value> {
		private Text[] columns;
		private int[] columnIndex;
		private Text table;

		protected void setup(Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();

			String colStrs = conf.get("totem.migration.columnname"); // col1:col2:col3
			String[] tmpColumns = colStrs.split(":");
			columns = new Text[tmpColumns.length];

			int i;
			for (i = 0; i < tmpColumns.length; i++) {
				columns[i] = new Text(tmpColumns[i]);
			}

			table = new Text(conf.get("totem.migration.tablename"));
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context output)
				throws IOException, InterruptedException {
			// be careful with the timestamp... if you run on a cluster
			// where the time is whacked you may not see your updates in
			// accumulo if there is already an existing value with a later
			// timestamp in accumulo... so make sure ntp is running on the
			// cluster or consider using logical time... one options is
			// to let accumulo set the time
			// int index = 0;
			long timestamp = System.currentTimeMillis();

			for (Text value : values) {
				String[] colVals = value.toString().split("\\|");
				// int begin = 0, end;
				int i;
				// String[] colVals = vale
				/*
				 * String oneValue = value.toString(); for (i = 0 ; i <
				 * colVals.length; i++){ end = oneValue.indexOf('|', begin);
				 * if(end == -1 || begin >= end) colVals[i] = ""; else
				 * colVals[i] = oneValue.substring(begin, end); begin = end + 1;
				 * }
				 */

				for (i = 0; i < columns.length; i++) {
					Key outputKey = new Key(key, table, new Text(columns[i]),
							timestamp);
					Value outputValue = new Value(colVals[i].getBytes());
					output.write(outputKey, outputValue);
				}
			}
		}
	}

	static class Opts extends MapReduceClientOnRequiredTable {
		@Parameter(names = "--inputDir", required = true)
		String inputDir;
		@Parameter(names = "--workDir", required = true)
		String workDir;
		@Parameter(names = "--tableName", required = true)
		String tableName;
		@Parameter(names = "--columnName", required = true)
		String columnName;
		@Parameter(names = "--keyColumn", required = true)
		String keycolumn;
		@Parameter(names = "--reduceNum", required = true)
		String reducenum;
	}

	@Override
	public int run(String[] args) {
		Opts opts = new Opts();
		opts.parseArgs(BulkLoad.class.getName(), args);

		Configuration conf = getConf();
		PrintStream out = null;
		try {
			conf.set("totem.migration.tablename", opts.tableName);
			conf.set("totem.migration.columnname", opts.columnName);
			conf.set("totem.migration.keycolumn", opts.keycolumn);
			// conf.set("mapreduce.job.reduces", opts.reducenum);
			Job job = Job.getInstance(conf);
			// job.setNumReduceTasks(Integer.parseInt(opts.reducenum));
			job.setJobName("Bulk Load");
			job.setJarByClass(this.getClass());

			job.setInputFormatClass(TextInputFormat.class);

			job.setMapperClass(MapClass.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setReducerClass(ReduceClass.class);
			job.setOutputFormatClass(AccumuloFileOutputFormat.class);
			opts.setAccumuloConfigs(job);

			Connector connector = opts.getConnector();

			Path hdfsInput = new Path(opts.workDir + "/input");
			TextInputFormat.setInputPaths(job, hdfsInput);
			AccumuloFileOutputFormat.setOutputPath(job,
					new Path(opts.workDir + "/files"));

			FileSystem fs = FileSystem.get(conf);
			out = new PrintStream(new BufferedOutputStream(
					fs.create(new Path(opts.workDir + "/splits.txt"))));

			Collection<Text> splits = connector.tableOperations()
					.listSplits(opts.getTableName(), 100);
			for (Text split : splits)
				out.println(
						Base64.encodeBase64String(TextUtil.getBytes(split)));

			job.setNumReduceTasks(splits.size() + 1);
			out.close();

			job.setPartitionerClass(RangePartitioner.class);
			RangePartitioner.setSplitFile(job, opts.workDir + "/splits.txt");

			System.out.println("Uploading Data...");

			fs.mkdirs(hdfsInput);
			Path locaInput = new Path(opts.inputDir);
			FileStatus[] list = FileSystem.getLocal(conf).listStatus(locaInput);
			for (FileStatus f : list) {
				// System.out.printf("name: %s, folder: %s, size: %d\n",
				// f.getPath(), f.isDir(), f.getLen());
				fs.copyFromLocalFile(f.getPath(), hdfsInput);
			}
			// fs.copyFromLocalFile(new +"/*"), hdfsInput);
			System.out.println("End of Uploading Data");

			job.waitForCompletion(true);
			Path failures = new Path(opts.workDir, "failures");
			fs.delete(failures, true);
			fs.mkdirs(new Path(opts.workDir, "failures"));

			fs.deleteOnExit(new Path(opts.workDir));

			// With HDFS permissions on, we need to make sure the Accumulo user
			// can read/move the rfiles
			FsShell fsShell = new FsShell(conf);
			fsShell.run(new String[] { "-chmod", "-R", "777", opts.workDir });
			connector.tableOperations().importDirectory(opts.getTableName(),
					opts.workDir + "/files", opts.workDir + "/failures", false);

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (out != null)
				out.close();
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BulkLoad(), args);
		System.exit(res);
	}
}