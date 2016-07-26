/**
 * 
 */
package com.rpamballa.cdc.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author revanthpamballa
 * 
 */
public class CDCRecord implements Writable {

     private Text value;

     private BooleanWritable isHiveRecord;

     private BooleanWritable isDeltaRecord;

     private IntWritable dateToCompare;

     public CDCRecord() {
          this.setValue("");
          this.setIsHiveRecord(false);
          this.setIsDeltaRecord(false);
          this.setDateToCompare(0);
     }

     public CDCRecord(String value, Integer dateToCompare, Boolean isHiveRecord, Boolean isDeltaRecord) {
          this.setValue(value);
          this.setDateToCompare(dateToCompare);
          this.setIsHiveRecord(isHiveRecord);
          this.setIsDeltaRecord(isDeltaRecord);
     }

     @Override
     public void readFields(DataInput in) throws IOException {
          value.readFields(in);
          dateToCompare.readFields(in);
          isHiveRecord.readFields(in);
          isDeltaRecord.readFields(in);

     }

     @Override
     public void write(DataOutput out) throws IOException {
          this.value.write(out);
          this.dateToCompare.write(out);
          this.isHiveRecord.write(out);
          this.isDeltaRecord.write(out);
     }

     @Override
     public int hashCode() {
          int prime = 31;
          return prime * (this.dateToCompare.get() + this.value.hashCode());
     }

     public void setValue(final String value) {
          this.value = new Text(value);
     }

     public String getValue() {
          return this.value.toString();
     }

     public Boolean getIsHiveRecord() {
          return isHiveRecord.get();
     }

     public void setIsHiveRecord(final Boolean isHiveRecord) {
          this.isHiveRecord = new BooleanWritable(isHiveRecord);
     }

     public Integer getDateToCompare() {
          return dateToCompare.get();
     }

     public void setDateToCompare(final Integer dateToCompare) {
          this.dateToCompare = new IntWritable(dateToCompare);
     }

     public Boolean getIsDeltaRecord() {
          return isDeltaRecord.get();
     }

     public void setIsDeltaRecord(final Boolean isDeltaRecord) {
          this.isDeltaRecord = new BooleanWritable(isDeltaRecord);
     }

}
