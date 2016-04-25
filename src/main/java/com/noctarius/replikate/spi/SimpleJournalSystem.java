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
package com.noctarius.replikate.spi;

import com.noctarius.replikate.Journal;
import com.noctarius.replikate.JournalBatch;
import com.noctarius.replikate.JournalConfiguration;
import com.noctarius.replikate.JournalEntry;
import com.noctarius.replikate.JournalListener;
import com.noctarius.replikate.JournalNamingStrategy;
import com.noctarius.replikate.JournalStrategy;
import com.noctarius.replikate.JournalSystem;
import com.noctarius.replikate.exceptions.JournalException;
import com.noctarius.replikate.impl.disk.DiskJournalFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleJournalSystem
        implements JournalSystem {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleJournalSystem.class);

    private final Set<Journal<?>> journals = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final ExecutorService listenerExecutorService;
    private final boolean externalExecutorService;

    public SimpleJournalSystem(ExecutorService listenerExecutorService) {
        this.externalExecutorService = listenerExecutorService != null;
        this.listenerExecutorService = this.externalExecutorService ? listenerExecutorService //
                : Executors.newSingleThreadExecutor(new NamedThreadFactory("RepliKate-Listener"));
    }

    @Override
    public <V> Journal<V> getJournal(String name, JournalConfiguration<V> configuration) {
        return getJournal(name, JournalStrategy.DiskJournal, configuration);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> Journal<V> getJournal(String name, JournalStrategy journalStrategy, JournalConfiguration<V> configuration) {
        Preconditions.notNull(journalStrategy, "journalStrategy");

        switch (journalStrategy) {
            case DiskJournal:
                return getJournal(name, DiskJournalFactory.defaultInstance(), configuration);

            case AMQPJournal:
                throw new UnsupportedOperationException("AMQP journal not yet implemented");
        }

        throw new IllegalStateException("Unknown JournalStrategy");
    }

    @Override
    public <V> Journal<V> getJournal(String name, JournalFactory<V> journalFactory, JournalConfiguration<V> configuration) {
        Preconditions.notNull(name, "name");
        Preconditions.notNull(configuration, "configuration");
        Preconditions.notNull(configuration.getEntryReader(), "configuration.entryReader");
        Preconditions.notNull(configuration.getEntryWriter(), "configuration.entryWriter");
        Preconditions.notNull(configuration.getListener(), "configuration.listener");
        Preconditions.notNull(configuration.getNamingStrategy(), "configuration.namingStrategy");
        Preconditions.notNull(configuration.getRecordIdGenerator(), "configuration.recordIdGenerator");

        Journal<V> journal = journalFactory.buildJournal(name, configuration, listenerExecutorService);
        return new JournalProxy<V>(journal);
    }

    @Override
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            journals.stream().forEach(j -> {
                try {
                    j.close();
                } catch (IOException e) {
                    LOGGER.error("Problem while shutdown of JournalSystem", e);
                }
            });
        }

        if (!externalExecutorService) {
            listenerExecutorService.shutdown();
        }
    }

    private class JournalProxy<V>
            implements Journal<V> {

        private final AtomicBoolean closed = new AtomicBoolean(false);

        private final Journal<V> journal;

        private JournalProxy(Journal<V> journal) {
            this.journal = journal;
            journals.add(journal);
        }

        @Override
        public String getName() {
            return journal.getName();
        }

        @Override
        public void appendEntry(JournalEntry<V> entry)
                throws JournalException {

            journal.appendEntry(entry);
        }

        @Override
        public void appendEntry(JournalEntry<V> entry, JournalListener<V> listener)
                throws JournalException {

            journal.appendEntry(entry, listener);
        }

        @Override
        public JournalBatch<V> startBatchProcess() {
            return journal.startBatchProcess();
        }

        @Override
        public JournalBatch<V> startBatchProcess(JournalListener<V> listener) {
            return journal.startBatchProcess(listener);
        }

        @Override
        public long getLastRecordId() {
            return journal.getLastRecordId();
        }

        @Override
        public long nextLogNumber() {
            return journal.nextLogNumber();
        }

        @Override
        public void close()
                throws IOException {

            if (!closed.compareAndSet(false, true)) {
                return;
            }

            try {
                journal.close();
            } finally {
                journals.remove(journal);
            }
        }

        @Override
        public JournalRecordIdGenerator getRecordIdGenerator() {
            return journal.getRecordIdGenerator();
        }

        @Override
        public JournalEntryReader<V> getReader() {
            return journal.getReader();
        }

        @Override
        public JournalEntryWriter<V> getWriter() {
            return journal.getWriter();
        }

        @Override
        public JournalNamingStrategy getNamingStrategy() {
            return journal.getNamingStrategy();
        }

        public Journal<V> getJournal() {
            return journal;
        }
    }

}
