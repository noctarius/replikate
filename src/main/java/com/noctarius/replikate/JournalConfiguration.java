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
package com.noctarius.replikate;

import com.noctarius.replikate.spi.JournalEntryReader;
import com.noctarius.replikate.spi.JournalEntryWriter;
import com.noctarius.replikate.spi.JournalRecordIdGenerator;

public class JournalConfiguration<V> {

    private JournalListener<V> listener;

    private JournalRecordIdGenerator recordIdGenerator;

    private JournalNamingStrategy namingStrategy;

    private JournalEntryReader<V> entryReader;

    private JournalEntryWriter<V> entryWriter;

    public JournalListener<V> getListener() {
        return listener;
    }

    public void setListener(JournalListener<V> listener) {
        this.listener = listener;
    }

    public JournalRecordIdGenerator getRecordIdGenerator() {
        return recordIdGenerator;
    }

    public void setRecordIdGenerator(JournalRecordIdGenerator recordIdGenerator) {
        this.recordIdGenerator = recordIdGenerator;
    }

    public JournalNamingStrategy getNamingStrategy() {
        return namingStrategy;
    }

    public void setNamingStrategy(JournalNamingStrategy namingStrategy) {
        this.namingStrategy = namingStrategy;
    }

    public JournalEntryReader<V> getEntryReader() {
        return entryReader;
    }

    public void setEntryReader(JournalEntryReader<V> entryReader) {
        this.entryReader = entryReader;
    }

    public JournalEntryWriter<V> getEntryWriter() {
        return entryWriter;
    }

    public void setEntryWriter(JournalEntryWriter<V> entryWriter) {
        this.entryWriter = entryWriter;
    }

}
