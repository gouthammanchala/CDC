/**
 * 
 */
package com.rpamballa.cdc.mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rpamballa.cdc.record.CDCRecord;

/**
 * @author Revanth
 * 
 */
public class DeltaFileMapper extends Mapper<Object, Text, Text, CDCRecord> {

     private static Logger LOG = LoggerFactory.getLogger(DeltaFileMapper.class);

     private String DELIMITER = ",";

     private CDCRecord idcRecord = new CDCRecord();

     private DateFormat DATE_FORMAT = new SimpleDateFormat("MM/dd/yyyy");

     private Integer idcIndex = new Integer(0);

     private String primaryKeys;

     @Override
     protected void setup(Context context) throws IOException, InterruptedException {
          super.setup(context);
          Configuration conf = context.getConfiguration();
          DELIMITER = conf.get("delimiter");
          DATE_FORMAT = new SimpleDateFormat(conf.get("cdcFormat"));
          idcIndex = conf.getInt("cdcIndex", 0);
          primaryKeys = conf.get("primaryKeyIndex");
     }

     @Override
     protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          context.write(new Text(getPrimaryKey(value)), mapDeltaRec(value));
     }

     private CDCRecord mapDeltaRec(final Text value) {
          idcRecord = new CDCRecord(value.toString(), getIDCColumn(value, idcIndex), new Boolean(false), new Boolean(
                    true));
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
          String[] valueTokens = value.toString().split(DELIMITER);
          StringBuilder keyBuilder = new StringBuilder();
          for (String token : tokens) {
               keyBuilder.append(valueTokens[Integer.parseInt(token)]);
          }
          return keyBuilder.toString();
     }

}
