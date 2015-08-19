/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.streams.converter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.streams.core.StreamsDatum;
import org.apache.streams.core.StreamsProcessor;
import org.apache.streams.jackson.StreamsJacksonMapper;
import org.apache.streams.pojo.extensions.ExtensionUtil;
import org.apache.streams.pojo.json.Activity;
import org.apache.streams.pojo.json.ActivityObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/**
 * TypeConverterProcessor converts between String json and jackson-compatible POJO objects.
 *
 * Activity is one supported jackson-compatible POJO, so JSON String and objects with structual similarities
 *   to Activity can be converted to Activity objects.
 *
 * However, conversion to Activity should probably use {@link ActivityConverterProcessor}
 *
 */
public class PromoteExtensionsProcessor implements StreamsProcessor, Serializable {

    private final static Logger LOGGER = LoggerFactory.getLogger(PromoteExtensionsProcessor.class);

    private List<String> formats = Lists.newArrayList();

    protected ObjectMapper mapper;

    protected Class outClass;

    @Override
    public List<StreamsDatum> process(StreamsDatum entry) {

        List<StreamsDatum> result = Lists.newLinkedList();
        Object doc = entry.getDocument();

        if( doc instanceof Activity) {
            ExtensionUtil.promoteExtensions((Activity)doc);
        } else if( doc instanceof ActivityObject) {
            ExtensionUtil.promoteExtensions((ActivityObject)doc);
        } else {
            LOGGER.warn("document wrong type: " + doc.getClass());
        }

        entry.setDocument(doc);
        result.add(entry);

        return result;
    }

    @Override
    public void prepare(Object configurationObject) {

    }

    @Override
    public void cleanUp() {

    }
};
