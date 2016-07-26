/**
 * 
 */
package com.rpamballa.cdc.utils;

/**
 * @author revanthpamballa
 * 
 */
public class BloomFilterUtils {

     public static int getOptimalBloomFilterSize(final int numOfRecs, final float f) {
          return (int) (-numOfRecs * (float) Math.log(f));
     }

     public static int getOptimalK(final float numElements, final float vectorSize) {
          return (int) Math.round(vectorSize * Math.log(2) / numElements);
     }
}
