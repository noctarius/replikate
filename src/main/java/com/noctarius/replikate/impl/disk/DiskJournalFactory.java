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
package com.noctarius.replikate.impl.disk;

import com.noctarius.replikate.Journal;
import com.noctarius.replikate.JournalConfiguration;
import com.noctarius.replikate.JournalListener;
import com.noctarius.replikate.JournalNamingStrategy;
import com.noctarius.replikate.exceptions.JournalConfigurationException;
import com.noctarius.replikate.spi.JournalEntryReader;
import com.noctarius.replikate.spi.JournalEntryWriter;
import com.noctarius.replikate.spi.JournalFactory;
import com.noctarius.replikate.spi.JournalRecordIdGenerator;
import com.noctarius.replikate.spi.Preconditions;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

public class DiskJournalFactory<V>
        implements JournalFactory<V> {

    public static final JournalFactory<?> DEFAULT_INSTANCE = new DiskJournalFactory<>();

    private static final int MIN_DISK_JOURNAL_FILE_SIZE = 1024;

    @Override
    public Journal<V> buildJournal(String name, JournalConfiguration<V> configuration, ExecutorService listenerExecutorService) {
        Preconditions.checkType(configuration, DiskJournalConfiguration.class, "configuration");
        DiskJournalConfiguration<V> diskConfig = (DiskJournalConfiguration<V>) configuration;

        Path journalingPath = diskConfig.getJournalPath();
        JournalListener<V> listener = diskConfig.getListener();
        int maxLogFileSize = diskConfig.getMaxLogFileSize();
        JournalRecordIdGenerator recordIdGenerator = diskConfig.getRecordIdGenerator();
        JournalEntryReader<V> entryReader = diskConfig.getEntryReader();
        JournalEntryWriter<V> entryWriter = diskConfig.getEntryWriter();
        JournalNamingStrategy namingStrategy = diskConfig.getNamingStrategy();

        Preconditions.notNull(journalingPath, "configuration.journalingPath");

        if (maxLogFileSize < MIN_DISK_JOURNAL_FILE_SIZE) {
            throw new IllegalArgumentException("configuration.maxLogFileSize must not be below " + MIN_DISK_JOURNAL_FILE_SIZE);
        }

        try {
            return new DiskJournal<>(name, journalingPath, listener, maxLogFileSize, recordIdGenerator, entryReader, entryWriter,
                    namingStrategy, listenerExecutorService);

        } catch (IOException e) {
            throw new JournalConfigurationException("Error while configuring the journal", e);
        }
    }

    public static <V> JournalFactory<V> defaultInstance() {
        return (JournalFactory<V>) DEFAULT_INSTANCE;
    }

}
