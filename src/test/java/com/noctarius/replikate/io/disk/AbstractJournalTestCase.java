/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package com.noctarius.replikate.io.disk;

import com.noctarius.replikate.JournalConfiguration;
import com.noctarius.replikate.JournalListener;
import com.noctarius.replikate.JournalNamingStrategy;
import com.noctarius.replikate.spi.JournalEntryReader;
import com.noctarius.replikate.spi.JournalEntryWriter;
import com.noctarius.replikate.spi.JournalRecordIdGenerator;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public abstract class AbstractJournalTestCase {

    protected File prepareJournalDirectory(String name)
            throws IOException {

        File path = new File("target/journals/" + name);
        if (path.exists() && path.isDirectory()) {
            Files.walkFileTree(path.toPath(), new SimpleFileVisitor<Path>() {

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                        throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                        throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        }

        path.mkdirs();
        return path;
    }

    protected <V> JournalConfiguration<V> buildDiskJournalConfiguration(Path journalingPath, int maxLogFileSize,
                                                                        JournalEntryReader<V> reader,
                                                                        JournalEntryWriter<V> writer, JournalListener<V> listener,
                                                                        JournalNamingStrategy namingStrategy,
                                                                        JournalRecordIdGenerator recordIdGenerator) {

        DiskJournalConfiguration<V> configuration = new DiskJournalConfiguration<>();
        configuration.setEntryReader(reader);
        configuration.setEntryWriter(writer);
        configuration.setJournalingPath(journalingPath);
        configuration.setListener(listener);
        configuration.setMaxLogFileSize(maxLogFileSize);
        configuration.setNamingStrategy(namingStrategy);
        configuration.setRecordIdGenerator(recordIdGenerator);

        return configuration;
    }

}
