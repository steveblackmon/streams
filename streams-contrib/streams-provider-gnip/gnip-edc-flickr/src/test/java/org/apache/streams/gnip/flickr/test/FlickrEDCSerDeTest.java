/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.streams.gnip.flickr.test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Tests serialization of FlickrEDC inputs
 */
@Ignore("ignore until test resources are available.")
public class FlickrEDCSerDeTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(FlickrEDCSerDeTest.class);

    private ObjectMapper mapper = new ObjectMapper();

    @Test
    public void Tests()   throws Exception
    {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, Boolean.FALSE);
        mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, Boolean.TRUE);
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, Boolean.TRUE);

        InputStream is = FlickrEDCSerDeTest.class.getResourceAsStream("/FlickrEDC.xml");
        if(is == null) LOGGER.debug("null");
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        XmlMapper xmlMapper = new XmlMapper();
        xmlMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, Boolean.FALSE);
        xmlMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, Boolean.TRUE);
        xmlMapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, Boolean.TRUE);
        xmlMapper.configure(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE, Boolean.FALSE);

        ObjectMapper jsonMapper = new ObjectMapper();

        try {
            while (br.ready()) {
                String line = br.readLine();

                Object activityObject = xmlMapper.readValue(line, Object.class);

                String jsonObject = jsonMapper.writeValueAsString(activityObject);
            }
        } catch( Exception e ) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
