package com.rpamballa.cdc.propconfig;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.rpamballa.cdc.utils.Constants;
import com.rpamballa.cdc.utils.PropConfig;

public class PropConfigTest {

     @Test
     public void testPropConfig() throws IOException {
          assertNotNull(PropConfig.INSTANCE.getProperty(Constants.THRIFT_URL));

     }

}
