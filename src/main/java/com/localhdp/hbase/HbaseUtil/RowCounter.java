package com.localhdp.hbase.HbaseUtil;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.base.Preconditions;

/**
 * A job with a a map and reduce phase to count cells in a table. The counter
 * lists the following stats for a given table:
 * 
 * <pre>
 * 1. Total number of rows in the table
 * 2. Total number of rows by first timestamp
 * </pre>
 *
 * The rowcounter takes two optional parameters one to use a user supplied
 * row/family/qualifier string to use in the report and second a regex based or
 * prefix based row filter to restrict the count operation to a limited subset
 * of rows from the table.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RowCounter {
	private static final Log LOG = LogFactory.getLog(RowCounter.class.getName());

	/**
	 * Name of this 'program'.
	 */
	static final String NAME = "RowCounter";

	/**
	 * Mapper that runs the count.
	 */
	static class CellCounterMapper extends TableMapper<Text, LongWritable> {
		/**
		 * Counter enumeration to count the actual rows.
		 */
		public static enum Counters {
			ROWS
		}

		/**
		 * Maps the data.
		 *
		 * @param row     The current table row key.
		 * @param values  The columns.
		 * @param context The current context.
		 * @throws IOException When something is broken with the data.
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 *      org.apache.hadoop.mapreduce.Mapper.Context)
		 */

		@Override
		public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
			Preconditions.checkState(values != null, "values passed to the map is null");
			// String currentFamilyName = null;
			// String currentQualifierName = null;
			String currentRowKey = null;
			Configuration config = context.getConfiguration();
			// String separator = config.get("ReportSeparator",":");
			try {
				context.getCounter(Counters.ROWS).increment(1);
				context.write(new Text("Total_ROWS"), new LongWritable(1));

				for (Cell value : values.listCells()) {
					currentRowKey = Bytes.toStringBinary(CellUtil.cloneRow(value));
					// String thisRowFamilyName = Bytes.toStringBinary(CellUtil.cloneFamily(value));
					// String thisRowQualifierName =
					// Bytes.toStringBinary(CellUtil.cloneQualifier(value));
					long timestamp = value.getTimestamp();
					LocalDateTime ceatedDateTime = LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.UTC);
					DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
					String createdDate = ceatedDateTime.format(formatter);

					context.write(new Text(createdDate), new LongWritable(1));

				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	static class IntSumReducer<Key> extends Reducer<Key, LongWritable, Key, LongWritable> {

		private LongWritable result = new LongWritable();

		public void reduce(Key key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	/**
	 * Sets up the actual job.
	 *
	 * @param conf The current configuration.
	 * @param args The command line parameters.
	 * @return The newly created job.
	 * @throws IOException When setting up the job fails.
	 */
	public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
		String tableName = args[0];
		Path outputDir = new Path(args[1]);
		String reportSeparatorString = (args.length > 2) ? args[2] : ":";
		conf.set("ReportSeparator", reportSeparatorString);
		Job job = new Job(conf, NAME + "_" + tableName);
		job.setJarByClass(RowCounter.class);
		Scan scan = getConfiguredScanForJob(conf, args);
		TableMapReduceUtil.initTableMapperJob(tableName, scan, CellCounterMapper.class, ImmutableBytesWritable.class,
				Result.class, job);
		job.setNumReduceTasks(getNumReducer(args));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		return job;
	}

	private static Scan getConfiguredScanForJob(Configuration conf, String[] args) throws IOException {
		Scan s = new Scan();
		// Set Scan Versions
		s.setMaxVersions(Integer.MAX_VALUE);
		s.setCacheBlocks(false);
		// Set Scan Column Family
		if (conf.get(TableInputFormat.SCAN_COLUMN_FAMILY) != null) {
			s.addFamily(Bytes.toBytes(conf.get(TableInputFormat.SCAN_COLUMN_FAMILY)));
		}
		// Set RowFilter or Prefix Filter if applicable.
		Filter rowFilter = getRowFilter(args);
		if (rowFilter != null) {
			LOG.info("Setting Row Filter for counter.");
			s.setFilter(rowFilter);
		}

		// Set TimeRange if defined
		long timeRange[] = getTimeRange(args);
		if (timeRange != null) {
			LOG.info("Setting TimeRange for counter.");
			s.setTimeRange(timeRange[0], timeRange[1]);
		}

		// Set family qualifier if defined
		Map<String, String> qualifier=getQualifier(args);
		if(qualifier.isEmpty()) {
		  for(Map.Entry<String, String> m : qualifier.entrySet()){  
			   System.out.println("Qualifier " + m.getKey()+":"+m.getValue());  
			   s.addColumn(Bytes.toBytes(m.getKey()), Bytes.toBytes(m.getValue()));
		  }  			
		}
		else {
			s.setFilter(new FirstKeyOnlyFilter());
		}


		return s;
	}

	private static Filter getRowFilter(String[] args) {
		Filter rowFilter = null;
		String filterCriteria = (args.length > 3) ? args[3] : null;
		if (filterCriteria == null)
			return null;
		if (filterCriteria.startsWith("^")) {
			String regexPattern = filterCriteria.substring(1, filterCriteria.length());
			rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regexPattern));
		} else {
			rowFilter = new PrefixFilter(Bytes.toBytes(filterCriteria));
		}
		return rowFilter;
	}

	private static long[] getTimeRange(String[] args) throws IOException {
		final String startTimeArgKey = "--starttime=";
		final String endTimeArgKey = "--endtime=";
		long startTime = 0L;
		long endTime = 0L;

		for (int i = 1; i < args.length; i++) {
			System.out.println("i:" + i + "arg[i]" + args[i]);
			if (args[i].startsWith(startTimeArgKey)) {
				startTime = Long.parseLong(args[i].substring(startTimeArgKey.length()));
			}
			if (args[i].startsWith(endTimeArgKey)) {
				endTime = Long.parseLong(args[i].substring(endTimeArgKey.length()));
			}
		}

		if (startTime == 0 && endTime == 0)
			return null;

		endTime = endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime;
		return new long[] { startTime, endTime };
	}

	private static Map<String, String> getQualifier(String[] args) throws IOException {
		final String qualifierArgKey = "--qualifiers=";
		Map<String, String> familyQualifier = new HashMap<String, String>();

		for (int i = 1; i < args.length; i++) {
			if (args[i].startsWith(qualifierArgKey)) {
				String qualifiers = args[i].substring(qualifierArgKey.length());
				if (qualifiers.length() > 2) {
					for (String columnName : qualifiers.split(",")) {
						String family = StringUtils.substringBefore(columnName, ":");
						String qualifier = StringUtils.substringAfter(columnName, ":");
						familyQualifier.put(family.trim(), qualifier.trim());
					}
				}

				break;
			}

		}

		return familyQualifier;
	}

	private static int getNumReducer(String[] args) throws IOException {
		final String numreducerArgKey = "--numreducer=";
		int numReducer = 1;

		for (int i = 1; i < args.length; i++) {
			System.out.println("i:" + i + "arg[i]" + args[i]);
			if (args[i].startsWith(numreducerArgKey)) {
				numReducer = Integer.parseInt(args[i].substring(numreducerArgKey.length()));
				break;
			}
		}

		return numReducer;
	}

	/**
	 * Main entry point.
	 *
	 * @param args The command line parameters.
	 * @throws Exception When running the job fails.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("ERROR: Wrong number of parameters: " + args.length);
			System.err.println("Usage: RowCounter ");
			System.err.println("       <tablename> <outputDir> <reportSeparator> [^[regex pattern] or "
					+ "[Prefix] for row filter]] --starttime=[starttime] --endtime=[endtime] --numreducer=[N]"
					+ "--qualifiers=[f1:c1,f2:c2");
			System.err.println("  Note: -D properties will be applied to the conf used. ");
			System.err.println("  Additionally, the following SCAN properties can be specified");
			System.err.println("  to get fine grained control on what is counted..");
			System.err.println("   -D " + TableInputFormat.SCAN_COLUMN_FAMILY + "=<familyName>");
			System.err.println(" <reportSeparator> parameter can be used to override the default report separator "
					+ "string : used to separate the rowId/column family name and qualifier name.");
			System.err.println(" [^[regex pattern] or [Prefix] parameter can be used to limit the cell counter count "
					+ "operation to a limited subset of rows from the table based on regex or prefix pattern.");
			System.exit(-1);
		}
		Job job = createSubmittableJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
