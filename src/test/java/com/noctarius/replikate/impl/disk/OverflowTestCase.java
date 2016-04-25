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
import com.noctarius.replikate.JournalBatch;
import com.noctarius.replikate.JournalConfiguration;
import com.noctarius.replikate.JournalEntry;
import com.noctarius.replikate.JournalListener;
import com.noctarius.replikate.JournalRecord;
import com.noctarius.replikate.JournalSystem;
import com.noctarius.replikate.SimpleJournalEntry;
import com.noctarius.replikate.exceptions.JournalException;
import com.noctarius.replikate.impl.disk.BasicDiskJournalTestCase.NamingStrategy;
import com.noctarius.replikate.spi.JournalEntryReader;
import com.noctarius.replikate.spi.JournalEntryWriter;
import com.noctarius.replikate.spi.JournalRecordIdGenerator;
import com.noctarius.replikate.spi.ReplayNotificationResult;
import org.junit.Test;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class OverflowTestCase
        extends AbstractJournalTestCase {

    @Test
    public void testSimpleFileOverflow()
            throws Exception {

        File path = prepareJournalDirectory("testSimpleFileOverflow");

        RecordIdGenerator recordIdGenerator = new RecordIdGenerator();
        JournalSystem journalSystem = JournalSystem.newJournalSystem();
        JournalConfiguration<byte[]> configuration = buildDiskJournalConfiguration(path.toPath(), 1024, new RecordReader(),
                new RecordWriter(), new FlushListener(), new NamingStrategy(), recordIdGenerator);

        Journal<byte[]> journal = journalSystem.getJournal("testSimpleFileOverflow", configuration);

        JournalEntry<byte[]> record1 = buildTestRecord((byte) 1);
        JournalEntry<byte[]> record2 = buildTestRecord((byte) 2);
        JournalEntry<byte[]> record3 = buildTestRecord((byte) 3);

        journal.appendEntry(record1);
        journal.appendEntry(record2);

        // Here the journal should overflow
        journal.appendEntry(record3);

        journal.close();

        CountingFlushListener listener = new CountingFlushListener(ReplayNotificationResult.Except);
        configuration.setListener(listener);
        journal = journalSystem.getJournal("testSimpleFileOverflow", configuration);

        assertEquals(3, listener.count);

        JournalEntry<byte[]> result1 = listener.get(0);
        JournalEntry<byte[]> result2 = listener.get(1);
        JournalEntry<byte[]> result3 = listener.get(2);

        assertEquals(record1, result1);
        assertEquals(record2, result2);
        assertEquals(record3, result3);
    }

    @Test
    public void testMultipleSimpleFileOverflow()
            throws Exception {

        File path = prepareJournalDirectory("testMultipleSimpleFileOverflow");

        RecordIdGenerator recordIdGenerator = new RecordIdGenerator();
        JournalSystem journalSystem = JournalSystem.newJournalSystem();
        JournalConfiguration<byte[]> configuration = buildDiskJournalConfiguration(path.toPath(), 4096, new RecordReader(),
                new RecordWriter(), new FlushListener(), new NamingStrategy(), recordIdGenerator);

        Journal<byte[]> journal = journalSystem.getJournal("testMultipleSimpleFileOverflow", configuration);

        @SuppressWarnings("unchecked") JournalEntry<byte[]>[] records = new JournalEntry[50];
        for (int i = 0; i < records.length; i++) {
            records[i] = buildTestRecord((byte) i);
            journal.appendEntry(records[i]);
        }

        journal.close();

        CountingFlushListener listener = new CountingFlushListener(ReplayNotificationResult.Except);
        configuration.setListener(listener);
        journal = journalSystem.getJournal("testMultipleSimpleFileOverflow", configuration);

        assertEquals(records.length, listener.count);

        for (int i = 0; i < records.length; i++) {
            JournalEntry<byte[]> result = listener.get(i);
            assertEquals(records[i], result);
        }
    }

    @Test
    public void testFullFileOverflow()
            throws Exception {

        File path = prepareJournalDirectory("testFullFileOverflow");

        RecordIdGenerator recordIdGenerator = new RecordIdGenerator();
        JournalSystem journalSystem = JournalSystem.newJournalSystem();
        JournalConfiguration<byte[]> configuration = buildDiskJournalConfiguration(path.toPath(), 1024, new RecordReader(),
                new RecordWriter(), new FlushListener(), new NamingStrategy(), recordIdGenerator);

        Journal<byte[]> journal = journalSystem.getJournal("testFullFileOverflow", configuration);

        @SuppressWarnings("unchecked") JournalEntry<byte[]>[] records = new JournalEntry[5];
        for (int i = 0; i < records.length; i++) {
            // Generate special overflow entry on index == 2
            records[i] = buildTestRecord(i == 2 ? 1024 : 400, (byte) i);
        }

        journal.appendEntry(records[0]);
        journal.appendEntry(records[1]);
        journal.appendEntry(records[2]);
        journal.appendEntry(records[3]);
        journal.appendEntry(records[4]);

        journal.close();

        // 3 journal files needs to be generated
        File[] files = path.listFiles();
        assertEquals(3, files.length);

        // "journal-2" needs to be overflow file
        RandomAccessFile raf = new RandomAccessFile(new File(path, "journal-2"), "r");
        DiskJournalFileHeader header = DiskJournalIOUtils.readHeader(raf);
        assertEquals(DiskJournal.JOURNAL_FILE_TYPE_OVERFLOW, header.getType());
        raf.close();

        CountingFlushListener listener = new CountingFlushListener(ReplayNotificationResult.Except);
        configuration.setListener(listener);
        journal = journalSystem.getJournal("testFullFileOverflow", configuration);

        assertEquals(records.length, listener.count);

        for (int i = 0; i < records.length; i++) {
            JournalEntry<byte[]> result = listener.get(i);
            assertEquals(records[i], result);
        }
    }

    private SimpleJournalEntry<byte[]> buildTestRecord(byte type) {
        return buildTestRecord(400, type);
    }

    private SimpleJournalEntry<byte[]> buildTestRecord(int dataLength, byte type) {
        Random random = new Random(-System.nanoTime());
        byte[] data = new byte[dataLength];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) random.nextInt(255);
        }
        return new SimpleJournalEntry<>(data, type);
    }

    public static class RecordReader
            implements JournalEntryReader<byte[]> {

        @Override
        public JournalEntry<byte[]> readJournalEntry(long recordId, byte type, byte[] data)
                throws IOException {

            return new SimpleJournalEntry<>(data, type);
        }
    }

    public static class RecordWriter
            implements JournalEntryWriter<byte[]> {

        @Override
        public void writeJournalEntry(JournalEntry<byte[]> entry, DataOutput out)
                throws IOException {

            out.write(entry.getValue());
        }

        @Override
        public int estimateRecordSize(JournalEntry<byte[]> entry) {
            return entry.getValue() == null ? 0 : entry.getValue().length;
        }

        @Override
        public boolean isRecordSizeEstimateable() {
            return true;
        }
    }

    public static class FlushListener
            implements JournalListener<byte[]> {

        @Override
        public void onCommit(JournalRecord<byte[]> record) {
            System.out.println("flushed: " + record);
        }

        @Override
        public void onFailure(JournalEntry<byte[]> entry, JournalException cause) {
            System.out.println("failed: " + entry);
        }

        @Override
        public void onFailure(JournalBatch<byte[]> journalBatch, JournalException cause) {
            System.out.println("failed: " + journalBatch);
        }

        @Override
        public ReplayNotificationResult onReplaySuspiciousRecordId(Journal<byte[]> journal, JournalRecord<byte[]> lastRecord,
                                                                   JournalRecord<byte[]> nextRecord) {
            return ReplayNotificationResult.Continue;
        }

        @Override
        public ReplayNotificationResult onReplayRecordId(Journal<byte[]> journal, JournalRecord<byte[]> record) {
            return ReplayNotificationResult.Continue;
        }
    }

    public static class RecordIdGenerator
            implements JournalRecordIdGenerator {

        private long recordId = 0;

        @Override
        public synchronized long nextRecordId() {
            return ++recordId;
        }

        @Override
        public long lastGeneratedRecordId() {
            return recordId;
        }

        @Override
        public void notifyHighestJournalRecordId(long recordId) {
            this.recordId = recordId;
        }
    }

    public static class CountingFlushListener
            extends FlushListener {

        private int count;

        private final List<JournalRecord<byte[]>> records = new ArrayList<>();

        private final ReplayNotificationResult missingRecordIdResult;

        public CountingFlushListener() {
            this(ReplayNotificationResult.Continue);
        }

        public CountingFlushListener(ReplayNotificationResult missingRecordIdResult) {
            this.missingRecordIdResult = missingRecordIdResult;
        }

        @Override
        public ReplayNotificationResult onReplaySuspiciousRecordId(Journal<byte[]> journal, JournalRecord<byte[]> lastRecord,
                                                                   JournalRecord<byte[]> nextRecord) {
            return missingRecordIdResult;
        }

        @Override
        public ReplayNotificationResult onReplayRecordId(Journal<byte[]> journal, JournalRecord<byte[]> record) {
            records.add(record);
            count++;
            return super.onReplayRecordId(journal, record);
        }

        public int getCount() {
            return count;
        }

        public JournalEntry<byte[]> get(int index) {
            return records.get(index).getJournalEntry();
        }
    }

}
