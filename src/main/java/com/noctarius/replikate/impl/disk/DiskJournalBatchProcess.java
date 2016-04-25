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

import com.noctarius.replikate.JournalBatch;
import com.noctarius.replikate.JournalEntry;
import com.noctarius.replikate.JournalListener;
import com.noctarius.replikate.exceptions.JournalException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.noctarius.replikate.impl.disk.DiskJournalIOUtils.prepareJournalEntry;

class DiskJournalBatchProcess<V>
        implements JournalBatch<V> {

    private final List<DiskJournalEntryFacade<V>> entries = new LinkedList<>();
    private final AtomicBoolean committed = new AtomicBoolean(false);

    private final JournalListener<V> listener;
    private final DiskJournal<V> journal;

    private volatile int dataSize = 0;

    DiskJournalBatchProcess(DiskJournal<V> journal, JournalListener<V> listener) {
        this.journal = journal;
        this.listener = listener;
    }

    @Override
    public void appendEntry(JournalEntry<V> entry)
            throws JournalException {

        try {
            DiskJournalEntryFacade<V> batchEntry = prepareJournalEntry(entry, journal.getWriter());
            entries.add(batchEntry);
            dataSize += batchEntry.cachedData.length;

        } catch (IOException e) {
            throw new JournalException("JournalEntry could not be added to the batch job", e);
        }
    }

    @Override
    public void commit()
            throws JournalException {

        if (!committed.compareAndSet(false, true)) {
            throw new JournalException("Batch already committed");
        }

        journal.commitBatchProcess(this, entries, dataSize, listener);
    }

    @Override
    public String toString() {
        return "DiskJournalBatchProcess [committed=" + committed + ", entries=" + entries + "]";
    }

}
