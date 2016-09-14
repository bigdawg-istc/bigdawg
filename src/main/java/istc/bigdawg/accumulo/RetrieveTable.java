package istc.bigdawg.accumulo;

import java.io.IOException;
import java.util.HashSet;

import org.apache.accumulo.core.cli.MapReduceClientOnRequiredTable;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.Parameter;

/**
 * Takes a table and outputs the specified column to a set of part files on hdfs
 * {@code accumulo accumulo.examples.mapreduce.TableToFile <username> 
 * <password> <tablename> <column> <hdfs-output-path>}
 */
public class RetrieveTable extends Configured implements Tool {

	static class Opts extends MapReduceClientOnRequiredTable {
		@Parameter(names = "--workDir", description = "work dir in hdfs", required = true)
		String workDir;
		@Parameter(names = "--outputDir", description = "output dir in local", required = true)
		String outputDir;
		@Parameter(names = "--columns", description = "columns to extract, in cf:cq{,cf:cq,...} form", required = true)
		String columns;
		@Parameter(names = "--mapNum", description = "map number", required = true)
		String mapNum;

	}

	/**
	 * The Mapper class that given a row number, will generate the appropriate
	 * output line.
	 */
	public static class TTFMapper
			extends Mapper<Key, Value, NullWritable, Text> {
		private int columnCount;
		private int count;
		private Text rowKey;
		private StringBuffer outputValue;

		protected void setup(Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			columnCount = conf.getInt("totem.migration.retrieve.columncount",
					0);
			rowKey = null;
			count = 0;
		}

		@Override
		public void map(Key row, Value data, Context context)
				throws IOException, InterruptedException {
			// Map.Entry<Key,Value> entry = new
			// SimpleImmutableEntry<Key,Value>(row, data);
			// context.write(NullWritable.get(), new
			// Text(DefaultFormatter.formatEntry(entry, false)));
			// context.setStatus("Outputed Value");
			Text tmpKey = row.getRow();
			if (rowKey == null) {
				count = 0;
				outputValue = new StringBuffer();
				rowKey = tmpKey;
				outputValue.append(rowKey.toString());
			}

			// if (rowKey.compareTo(tmpKey) == 0){
			count++;
			outputValue.append('|');
			outputValue.append(data.toString());
			if (count == columnCount) {
				context.write(NullWritable.get(),
						new Text(outputValue.toString()));
				rowKey = null;
			}
			// }
			// context.write(NullWritable.get(), new Text(columnCount + " " +
			// rowKey.toString() + " " + count + " " + outputValue.toString()));
		}
	}

	@Override
	public int run(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException, AccumuloSecurityException {
		Opts opts = new Opts();
		opts.parseArgs(getClass().getName(), args);
		String[] columns = opts.columns.split(",");

		Configuration conf = getConf();
		conf.setInt("totem.migration.retrieve.columncount", columns.length);
		conf.set("mapreduce.job.maps", opts.mapNum);
		Job job = Job.getInstance(conf);
		job.setJobName(this.getClass().getSimpleName() + "_"
				+ System.currentTimeMillis());
		job.setJarByClass(this.getClass());

		job.setInputFormatClass(AccumuloInputFormat.class);
		opts.setAccumuloConfigs(job);

		HashSet<Pair<Text, Text>> columnsToFetch = new HashSet<Pair<Text, Text>>();
		for (String col : columns) {
			int idx = col.indexOf(":");
			Text cf = new Text(idx < 0 ? col : col.substring(0, idx));
			Text cq = idx < 0 ? null : new Text(col.substring(idx + 1));
			if (cf.getLength() > 0)
				columnsToFetch.add(new Pair<Text, Text>(cf, cq));
		}
		if (!columnsToFetch.isEmpty())
			AccumuloInputFormat.fetchColumns(job, columnsToFetch);

		job.setMapperClass(TTFMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(0);

		job.setOutputFormatClass(TextOutputFormat.class);
		Path outDir = new Path(opts.workDir + "/output");
		TextOutputFormat.setOutputPath(job, outDir);

		job.waitForCompletion(true);

		FileSystem fs = FileSystem.get(conf);
		FileStatus[] list = fs.listStatus(outDir);
		for (FileStatus f : list) {
			fs.copyToLocalFile(f.getPath(), new Path(opts.outputDir));
		}
		fs.deleteOnExit(outDir);

		return job.isSuccessful() ? 0 : 1;
	}

	/**
	 *
	 * @param args
	 *            instanceName zookeepers username password table columns
	 *            outputpath
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new RetrieveTable(), args);
	}
}
