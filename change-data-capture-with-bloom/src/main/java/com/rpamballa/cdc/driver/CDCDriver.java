/**
 * 
 */
package com.rpamballa.cdc.driver;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.rpamballa.cdc.mapper.DeltaFileMapper;
import com.rpamballa.cdc.mapper.HiveTableMapper;
import com.rpamballa.cdc.record.CDCRecord;
import com.rpamballa.cdc.reducer.CDCReducer;
import com.rpamballa.cdc.utils.BloomFilterUtils;
import com.rpamballa.cdc.utils.Constants;
import com.rpamballa.cdc.utils.JobConfigurationHelper;
import com.rpamballa.cdc.utils.PropConfig;

/**
 * @author revanthpamballa
 * 
 */
public class CDCDriver extends Configured implements Tool {

     private static Logger LOG = Logger.getLogger(CDCDriver.class);

     private Properties jobParams = new Properties();

     private HiveMetaStoreClient hiveClient;

     private List<Path> inputPaths;

     private Path outPath;

     private Path activeTablePath;

     private Path filterPath;

     private Path inActiveTablePath;

     private void setUpHiveClient() throws IOException, MetaException {
          HiveConf conf = new HiveConf();
          String metastoreUrl = PropConfig.INSTANCE.getProperty(Constants.THRIFT_URL);
          if (null == metastoreUrl) {
               metastoreUrl = "thrift:///";
          }
          conf.set("hive.metastore.uris", metastoreUrl);
          hiveClient = new HiveMetaStoreClient(conf);

     }

     public void init(final String jobParamsToSet) throws FileNotFoundException, IOException, NoSuchObjectException,
               TException {
          setUpHiveClient();
          jobParams.load(new FileInputStream(jobParamsToSet));
          inputPaths = new ArrayList<Path>();
          inputPaths.add(new Path(this.jobParams.getProperty(Constants.F_INPUT_PATH)));
          filterPath = new Path(this.jobParams.getProperty(Constants.F_MR_OUTPUT_PATH) + Path.SEPARATOR + "filter"
                    + Calendar.getInstance().getTimeInMillis());
          outPath = new Path(this.jobParams.getProperty(Constants.F_MR_OUTPUT_PATH) + Path.SEPARATOR
                    + this.jobParams.getProperty(Constants.F_ACTIVE_TABLE_NAME));
          activeTablePath = getHiveTablePath(this.jobParams.getProperty(Constants.F_DATABASE_NAME),
                    this.jobParams.getProperty(Constants.F_ACTIVE_TABLE_NAME));
          inActiveTablePath = getHiveTablePath(this.jobParams.getProperty(Constants.F_DATABASE_NAME),
                    this.jobParams.getProperty(Constants.F_INACTIVE_TABLE_NAME));
          inputPaths.add(activeTablePath);

     }

     public Path getHiveTablePath(final String dbName, final String tableName) throws MetaException,
               NoSuchObjectException, TException {
          Table table = this.hiveClient.getTable(dbName, tableName);
          Path hiveInputPath = new Path(table.getSd().getLocation());
          return hiveInputPath;

     }

     @Override
     public int run(String[] args) throws Exception {
          Configuration conf = getConf();

          FileSystem dfs = FileSystem.get(conf);
          init(args[0]);
          trainIDCFilter(conf, Integer.parseInt(this.jobParams.getProperty(Constants.F_BLOOM_FILTER_IP_RECS)),
                    Float.parseFloat(this.jobParams.getProperty(Constants.F_BLOOM_FILTER_FPR)));
          DistributedCache.addCacheFile(filterPath.toUri(), conf);
          conf.set("delimiter", this.jobParams.getProperty(Constants.F_DELIMITER));
          conf.set("cdcIndex", this.jobParams.getProperty(Constants.F_CDC_COLUMN_INDEX));
          conf.set("cdcFormat", this.jobParams.getProperty(Constants.F_CDC_COLUMN_FORMAT));
          conf.set("primaryKeyIndex", this.jobParams.getProperty(Constants.F_PRIMARY_KEY_INDEXES));
          Job job = Job.getInstance(conf, "CDCJob");
          job.setJarByClass(CDCDriver.class);
          MultipleInputs.addInputPath(job, new Path(this.jobParams.getProperty(Constants.F_INPUT_PATH)),
                    TextInputFormat.class, DeltaFileMapper.class);
          MultipleInputs.addInputPath(job, activeTablePath, TextInputFormat.class, HiveTableMapper.class);

          job.setMapOutputKeyClass(Text.class);
          job.setMapOutputValueClass(CDCRecord.class);
          job.setOutputKeyClass(NullWritable.class);
          job.setOutputValueClass(Text.class);
          job.setReducerClass(CDCReducer.class);

          job.setNumReduceTasks(JobConfigurationHelper.getNumberOfReducers(dfs, inputPaths));
          FileOutputFormat.setOutputPath(job, this.outPath);
          MultipleOutputs.addNamedOutput(job, Constants.ACTIVE_OUT, TextOutputFormat.class, NullWritable.class,
                    Text.class);
          MultipleOutputs.addNamedOutput(job, Constants.INACTIVE_OUT, TextOutputFormat.class, NullWritable.class,
                    Text.class);
          boolean jobCompleted = false;
          jobCompleted = job.waitForCompletion(true);
          if (jobCompleted) {
               moveOutFilesToTable(conf);
               deleteFiles(conf);
          }
          return jobCompleted ? 0 : 1;
     }

     private void deleteFiles(final Configuration conf) throws IOException {
          FileSystem dfs = FileSystem.get(conf);
          LOG.info("DELETING MR output path " + outPath);
          dfs.delete(outPath, true);
          LOG.info("DELETING BLOOM FILTER " + filterPath);
          dfs.delete(filterPath, false);
     }

     private void moveOutFilesToTable(final Configuration conf) throws IOException, IllegalArgumentException,
               MetaException, NoSuchObjectException, TException {
          FileSystem dfs = FileSystem.get(conf);
          FileStatus[] activeFiles = dfs.globStatus(new Path(outPath.toString() + Path.SEPARATOR + Constants.ACTIVE_OUT
                    + "*"));
          FileStatus[] inActiveFiles = dfs.globStatus(new Path(outPath.toString() + Path.SEPARATOR
                    + Constants.INACTIVE_OUT + "*"));
          FileStatus[] currentFiles = dfs.listStatus(activeTablePath);
          renameFiles(inActiveFiles, inActiveTablePath, dfs);
          for (FileStatus currentFile : currentFiles) {
               dfs.delete(currentFile.getPath(), false);
          }
          renameFiles(activeFiles, activeTablePath, dfs);
     }

     private void renameFiles(final FileStatus[] files, final Path path, final FileSystem dfs)
               throws IllegalArgumentException, IOException {
          for (FileStatus activeFile : files) {
               String fileName = activeFile.getPath().getName() + Calendar.getInstance().getTimeInMillis();
               dfs.rename(activeFile.getPath(), new Path(path.toString() + Path.SEPARATOR + fileName));
          }
     }

     private void trainIDCFilter(final Configuration conf, final Integer noOfElements, final Float fpr)
               throws IOException {
          int vectorsize = BloomFilterUtils.getOptimalBloomFilterSize(noOfElements, fpr);
          int optimalK = BloomFilterUtils.getOptimalK((float) noOfElements, (float) vectorsize);
          BloomFilter filter = new BloomFilter(vectorsize, Math.max(1, optimalK), Hash.MURMUR_HASH);
          LOG.info("TRAINING IDC filter with vector size " + vectorsize + " nbHASH " + optimalK);
          int numElements = 0;
          FileSystem fs = FileSystem.get(conf);
          String line = null;
          for (FileStatus status : fs.listStatus(new Path(this.jobParams.getProperty(Constants.F_INPUT_PATH)))) {
               BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
               while ((line = reader.readLine()) != null) {
                    byte[] key = getPrimaryKey(line);
                    filter.add(new Key(key));
                    ++numElements;
               }
               reader.close();
          }
          LOG.info("Trained BLOOM FILTER FOR " + numElements + " entries" + "vectorsize" + vectorsize + " K-factor "
                    + optimalK);
          FSDataOutputStream out = fs.create(filterPath);
          filter.write(out);
          out.flush();
          out.close();
     }

     private byte[] getPrimaryKey(final String line) {
          String[] tokens = this.jobParams.getProperty(Constants.F_PRIMARY_KEY_INDEXES).split(",");
          String[] valuetokens = line.toString().split(this.jobParams.getProperty(Constants.F_DELIMITER));
          StringBuilder keyBuilder = new StringBuilder();
          for (String token : tokens) {
               keyBuilder.append(valuetokens[Integer.parseInt(token)]);
          }
          return keyBuilder.toString().getBytes();
     }

     public static void main(String[] args) throws Exception {
          try {
               int res = ToolRunner.run(new Configuration(), new CDCDriver(), args);
               System.exit(res);
          } catch (Exception e) {
               LOG.error(e.getMessage());
               throw e;
          }
     }

     public Properties getJobParams() {
          return this.jobParams;
     }

}
