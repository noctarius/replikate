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

import com.noctarius.replikate.exceptions.JournalException;
import com.noctarius.replikate.spi.JournalEntryReader;
import com.noctarius.replikate.spi.JournalEntryWriter;
import com.noctarius.replikate.spi.JournalRecordIdGenerator;

import java.io.IOException;

public interface Journal<V> {

    public static final int JOURNAL_VERSION = 1;

    String getName();

    void appendEntry(JournalEntry<V> entry)
            throws JournalException;

    void appendEntry(JournalEntry<V> entry, JournalListener<V> listener)
            throws JournalException;

    void appendEntrySynchronous(JournalEntry<V> entry)
            throws JournalException;

    void appendEntrySynchronous(JournalEntry<V> entry, JournalListener<V> listener)
            throws JournalException;

    JournalBatch<V> startBatchProcess();

    JournalBatch<V> startBatchProcess(JournalListener<V> listener);

    long getLastRecordId();

    long nextLogNumber();

    void close()
            throws IOException;

    JournalRecordIdGenerator getRecordIdGenerator();

    JournalEntryReader<V> getReader();

    JournalEntryWriter<V> getWriter();

    JournalNamingStrategy getNamingStrategy();

}
