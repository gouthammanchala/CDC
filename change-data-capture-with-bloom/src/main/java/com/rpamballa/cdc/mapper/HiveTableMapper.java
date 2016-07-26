/**
 * 
 */
package com.rpamballa.cdc.mapper;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rpamballa.cdc.record.CDCRecord;
import com.rpamballa.cdc.utils.Constants;

/**
 * @author Revanth
 * 
 */
public class HiveTableMapper extends Mapper<Object, Text, Text, CDCRecord> {

     private static Logger LOG = LoggerFactory.getLogger(HiveTableMapper.class);

     private String DELIMITER = ",";

     private CDCRecord idcRecord = new CDCRecord();

     private DateFormat DATE_FORMAT = new SimpleDateFormat("MM/dd/yyyy");

     private Integer idcIndex;

     private String primaryKeys;

     private String keyVal;

     private BloomFilter filter = new BloomFilter();

     private MultipleOutputs out;

     @Override
     protected void setup(Context context) throws IOException, InterruptedException {
          super.setup(context);
          Configuration conf = context.getConfiguration();
          out = new MultipleOutputs(context);
          Path[] files = DistributedCache.getLocalCacheFiles(conf);
          URI filterURI = null;
          LOG.info(" LOCAL PATHS SIZE " + files.length + " FILE NAME " + files[0]);
          filterURI = files[0].toUri();
          DataInputStream in = new DataInputStream(new FileInputStream(filterURI.getPath()));
          filter.readFields(in);
          in.close();
          DELIMITER = conf.get("delimiter");
          DATE_FORMAT = new SimpleDateFormat(conf.get("cdcFormat"));
          idcIndex = conf.getInt("cdcIndex", 0);
          primaryKeys = conf.get("primaryKeyIndex");
     }

     @Override
     protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          keyVal = getPrimaryKey(value);
          if (filter.membershipTest(new Key(keyVal.getBytes()))) {
               context.write(new Text(keyVal), mapDeltaRec(value));
          } else {
               out.write(Constants.ACTIVE_OUT, NullWritable.get(), value);
          }
     }

     private CDCRecord mapDeltaRec(final Text value) {
          idcRecord = new CDCRecord(value.toString(), getIDCColumn(value, idcIndex), new Boolean(true), new Boolean(
                    false));
          return idcRecord;
     }

     private Integer getIDCColumn(final Text value, final int index) {
          String[] tokens = value.toString().split(DELIMITER);
          Integer idcDate = null;
          try {
               idcDate = (int) DATE_FORMAT.parse(tokens[index]).getTime();
          } catch (ParseException e) {
               LOG.error("ERROR PARSING DATE FROM VALUE " + value + " AT INDEX " + index);
          }
          return idcDate;
     }

     private String getPrimaryKey(final Text value) {
          String[] tokens = primaryKeys.split(",");
          String[] valuetokens = value.toString().split(DELIMITER);
          StringBuilder keyBuilder = new StringBuilder();
          for (String token : tokens) {
               keyBuilder.append(valuetokens[Integer.parseInt(token)]);
          }
          return keyBuilder.toString();
     }

     @Override
     protected void cleanup(Context context) throws IOException, InterruptedException {
          super.cleanup(context);
          out.close();
     }

}
