/**
 * 
 */
package com.rpamballa.cdc.record;

import java.util.Comparator;

/**
 * @author revanthpamballa
 * 
 */
public class CDCComparator implements Comparator<CDCRecord> {

     @Override
     public int compare(final CDCRecord o1, final CDCRecord o2) {
          return (-1) * (o1.getDateToCompare().compareTo(o2.getDateToCompare()));
     }
}
