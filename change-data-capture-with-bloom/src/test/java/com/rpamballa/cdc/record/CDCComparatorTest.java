/**
 * 
 */
package com.rpamballa.cdc.record;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

/**
 * @author revanthpamballa
 * 
 */
public class CDCComparatorTest {

     @Test
     public void testCompare() {
          CDCRecord rec1 = new CDCRecord("rec1", 1410054917, new Boolean(true), new Boolean(false));
          CDCRecord rec2 = new CDCRecord("rec2", 1410055382, new Boolean(false), new Boolean(true));

          List<CDCRecord> recs = new ArrayList<CDCRecord>();

          recs.add(rec1);
          recs.add(rec2);
          assertEquals("rec1", recs.get(0).getValue());
          Collections.sort(recs, new CDCComparator());

          assertEquals("rec2", recs.get(0).getValue());
     }

}
