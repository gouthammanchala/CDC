/**
 * 
 */
package com.rpamballa.cdc.utils;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author revanthpamballa
 * 
 */
public class JobConfigurationHelper {

     public static int getNumberOfReducers(final FileSystem dfs, final List<Path> inputPaths) throws IOException {
          long size = 0;
          int numReducers = 1;
          long bytesPerReducer = 10000000L;
          for (Path path : inputPaths) {
               size += dfs.getFileStatus(path).getLen();
          }
          return (int) Math.max(numReducers, size / bytesPerReducer);
     }

}
