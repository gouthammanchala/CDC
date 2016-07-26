package com.rpamballa.cdc.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rpamballa.cdc.record.CDCComparator;
import com.rpamballa.cdc.record.CDCRecord;
import com.rpamballa.cdc.utils.Constants;

public class CDCReducer extends Reducer<Text, CDCRecord, NullWritable, Text> {

     private static Logger LOG = LoggerFactory.getLogger(CDCReducer.class);

     private MultipleOutputs<NullWritable, Text> out;

     @Override
     protected void setup(Context context) throws IOException, InterruptedException {
          super.setup(context);
          out = new MultipleOutputs<NullWritable, Text>(context);
     }

     @Override
     protected void reduce(Text key, Iterable<CDCRecord> values, Context context) throws IOException,
               InterruptedException {
          List<CDCRecord> cdcRecs = new ArrayList<CDCRecord>();
          for (CDCRecord value : values) {
               cdcRecs.add(new CDCRecord(value.getValue(), value.getDateToCompare(), value.getIsHiveRecord(), value
                         .getIsDeltaRecord()));
          }
          Collections.sort(cdcRecs, new CDCComparator());
          out.write(Constants.ACTIVE_OUT, NullWritable.get(), new Text(cdcRecs.get(0).getValue()));
          if (cdcRecs.size() > 1) {
               for (int i = 1; i < cdcRecs.size(); i++) {
                    out.write(Constants.INACTIVE_OUT, NullWritable.get(), new Text(cdcRecs.get(i).getValue()));
               }
          }
     }

     @Override
     protected void cleanup(Context context) throws IOException, InterruptedException {
          super.cleanup(context);
          out.close();
     }

}
