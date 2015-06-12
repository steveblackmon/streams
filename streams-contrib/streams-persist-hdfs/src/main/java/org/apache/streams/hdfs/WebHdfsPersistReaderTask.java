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

package org.apache.streams.hdfs;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.hadoop.fs.FileStatus;
import org.apache.streams.core.DatumStatus;
import org.apache.streams.core.StreamsDatum;
import org.apache.streams.jackson.StreamsJacksonMapper;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class WebHdfsPersistReaderTask implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebHdfsPersistReaderTask.class);

    private WebHdfsPersistReader reader;

    public WebHdfsPersistReaderTask(WebHdfsPersistReader reader) {
        this.reader = reader;
    }

    @Override
    public void run() {

        for( FileStatus fileStatus : reader.status ) {
            BufferedReader bufferedReader;
            LOGGER.info("Found " + fileStatus.getPath().getName());
            if( fileStatus.isFile() && !fileStatus.getPath().getName().startsWith("_")) {
                LOGGER.info("Started Processing " + fileStatus.getPath().getName() + " expecting " + reader.hdfsConfiguration.getEncoding());
                try {
                    bufferedReader = new BufferedReader(new InputStreamReader(reader.client.open(fileStatus.getPath()), reader.hdfsConfiguration.getEncoding()));
                } catch (Exception e) {
                    LOGGER.error("Exception Opening " + fileStatus.getPath(), e.getMessage());
                    return;
                }

                String line = "";
                do{
                    try {
                        line = bufferedReader.readLine();
                        if( !Strings.isNullOrEmpty(line) ) {
                            reader.countersCurrent.incrementAttempt();
                            StreamsDatum entry = processLine(line);
                            if( entry != null ) {
                                write(entry);
                                reader.countersCurrent.incrementStatus(DatumStatus.SUCCESS);
                            } else {
                                LOGGER.warn("processLine failed");
                                reader.countersCurrent.incrementStatus(DatumStatus.FAIL);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        LOGGER.warn(e.getMessage());
                        reader.countersCurrent.incrementStatus(DatumStatus.FAIL);
                    }
                } while( !Strings.isNullOrEmpty(line) );
                LOGGER.info("Finished Processing " + fileStatus.getPath().getName());
                try {
                    bufferedReader.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.error(e.getMessage());
                }
            }
        }

        Uninterruptibles.sleepUninterruptibly(15, TimeUnit.SECONDS);
    }

    private void write( StreamsDatum entry ) {
        boolean success;
        do {
            synchronized( WebHdfsPersistReader.class ) {
                success = reader.persistQueue.offer(entry);
            }
            Thread.yield();
        }
        while( !success );
    }

    private StreamsDatum processLine(String line) {

        String[] fields = line.split(reader.hdfsConfiguration.getFieldDelimiter());

        if( fields.length == 0)
            return null;

        String id = null;
        DateTime ts = null;
        Map<String, Object> metadata = null;
        String json = null;

        if( reader.hdfsConfiguration.getFields().contains( HdfsConstants.DOC )
            && fields.length > reader.hdfsConfiguration.getFields().indexOf(HdfsConstants.DOC)) {
            json = fields[reader.hdfsConfiguration.getFields().indexOf(HdfsConstants.DOC)];
        }

        if( reader.hdfsConfiguration.getFields().contains( HdfsConstants.ID )
            && fields.length > reader.hdfsConfiguration.getFields().indexOf(HdfsConstants.ID)) {
            id = fields[reader.hdfsConfiguration.getFields().indexOf(HdfsConstants.ID)];
        }
        if( reader.hdfsConfiguration.getFields().contains( HdfsConstants.TS )
            && fields.length > reader.hdfsConfiguration.getFields().indexOf(HdfsConstants.TS)) {
            ts = parseTs(fields[reader.hdfsConfiguration.getFields().indexOf(HdfsConstants.TS)]);
        }
        if( reader.hdfsConfiguration.getFields().contains( HdfsConstants.META )
            && fields.length > reader.hdfsConfiguration.getFields().indexOf(HdfsConstants.META)) {
            metadata = parseMap(fields[reader.hdfsConfiguration.getFields().indexOf(HdfsConstants.META)]);
        }

        StreamsDatum datum = new StreamsDatum(json);
        datum.setId(id);
        datum.setTimestamp(ts);
        datum.setMetadata(metadata);

        return datum;

    }

    private DateTime parseTs(String field) {

        DateTime timestamp = null;
        try {
            long longts = Long.parseLong(field);
            timestamp = new DateTime(longts);
        } catch ( Exception e ) {}
        try {
            timestamp = reader.mapper.readValue(field, DateTime.class);
        } catch ( Exception e ) {}

        return timestamp;
    }

    private Map<String, Object> parseMap(String field) {

        Map<String, Object> metadata = null;

        try {
            JsonNode jsonNode = reader.mapper.readValue(field, JsonNode.class);
            metadata = reader.mapper.convertValue(jsonNode, Map.class);
        } catch (IOException e) {
            LOGGER.warn("failed in parseMap: " + e.getMessage());
        }
        return metadata;
    }
}
