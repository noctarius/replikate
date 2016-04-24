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

import com.noctarius.replikate.JournalEntry;
import com.noctarius.replikate.JournalRecord;

class DiskJournalRecord<V>
        implements JournalRecord<V> {

    private final JournalEntry<V> entry;

    private final long recordId;

    DiskJournalRecord(JournalEntry<V> entry, long recordId) {
        this.entry = entry;
        this.recordId = recordId;
    }

    @Override
    public int compareTo(JournalRecord<V> o) {
        return Long.valueOf(recordId).compareTo(o.getRecordId());
    }

    @Override
    public byte getType() {
        return entry.getType();
    }

    @Override
    public long getRecordId() {
        return recordId;
    }

    @Override
    public JournalEntry<V> getJournalEntry() {
        return entry;
    }

    @Override
    public String toString() {
        return "DiskJournalRecord [recordId=" + recordId + ", entry=" + entry + "]";
    }

}
