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

import com.noctarius.replikate.spi.JournalEntry;
import com.noctarius.replikate.impl.util.ByteArrayBufferOutputStream;
import com.noctarius.replikate.spi.JournalEntryReader;
import com.noctarius.replikate.spi.JournalEntryWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.Arrays;

enum DiskJournalIOUtils {
    ;

    private static final Logger LOGGER = LoggerFactory.getLogger(DiskJournalIOUtils.class);

    static DiskJournalFileHeader createJournal(RandomAccessFile raf, DiskJournalFileHeader header)
            throws IOException {

        long nanoSeconds = System.nanoTime();
        byte[] allocator = new byte[header.getMaxLogFileSize()];
        Arrays.fill(allocator, (byte) 0);

        try (ByteArrayBufferOutputStream buffer = new ByteArrayBufferOutputStream(allocator);
             DataOutputStream stream = new DataOutputStream(buffer)) {
            stream.write(DiskJournalFileHeader.MAGIC_NUMBER);
            stream.writeInt(header.getVersion());
            stream.writeInt(header.getMaxLogFileSize());
            stream.writeLong(header.getLogNumber());
            stream.write(header.getType());
            stream.writeInt(header.getFirstDataOffset());
        }

        raf.write(allocator);
        raf.seek(DiskJournal.JOURNAL_FILE_HEADER_SIZE);

        LOGGER.trace("DiskJournalIOUtils::createJournal took {}ns", (System.nanoTime() - nanoSeconds));

        return header;
    }

    static DiskJournalFileHeader readHeader(RandomAccessFile raf)
            throws IOException {

        // Read header and look for expected values
        byte[] magicNumber = new byte[4];
        raf.readFully(magicNumber);
        if (!Arrays.equals(magicNumber, DiskJournalFileHeader.MAGIC_NUMBER)) {
            throw new IllegalStateException("Given file no legal journal");
        }

        int version = raf.readInt();
        int maxLogFileSize = raf.readInt();
        long logFileNumber = raf.readLong();
        byte type = raf.readByte();
        int firstDataOffset = raf.readInt();
        return new DiskJournalFileHeader(version, maxLogFileSize, logFileNumber, type, firstDataOffset);
    }

    static <V> DiskJournalEntry<V> prepareJournalEntry(V entry, byte type, JournalEntryWriter<V> writer)
            throws IOException {

        long nanoSeconds = 0;
        if (LOGGER.isDebugEnabled()) {
            nanoSeconds = System.nanoTime();
        }

        DiskJournalEntry<V> journalEntry = buildDiskJournal(entry, type);
        if (journalEntry.cachedData == null) {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream(100);
                 DataOutputStream stream = new DataOutputStream(out)) {

                writer.writeJournalEntry(journalEntry, stream);
                journalEntry.cachedData = out.toByteArray();
            }

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("DiskJournalIOUtils::prepareJournalEntry tool {}ns", (System.nanoTime() - nanoSeconds));
            }
        }

        return journalEntry;
    }

    private static <V> DiskJournalEntry<V> buildDiskJournal(V entry, byte type) {
        return new DiskJournalEntry<>(entry, type);
    }

    static <V> void writeRecord(DiskJournalRecord<V> record, byte[] entryData, RandomAccessFile raf)
            throws IOException {

        long nanoSeconds = 0;
        if (LOGGER.isDebugEnabled()) {
            nanoSeconds = System.nanoTime();
        }

        int minSize = DiskJournal.JOURNAL_RECORD_HEADER_SIZE + entryData.length;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(minSize);
             DataOutputStream stream = new DataOutputStream(out)) {

            int recordLength = DiskJournal.JOURNAL_RECORD_HEADER_SIZE + entryData.length;

            stream.writeInt(recordLength);
            stream.writeLong(record.getRecordId());
            stream.writeByte(record.getType());
            stream.write(entryData);
            stream.writeInt(recordLength);
            raf.write(out.toByteArray());
        }

        LOGGER.trace("DiskJournalIOUtils::writeRecord took {}ns", (System.nanoTime() - nanoSeconds));
    }

    static <V> void prepareBulkRecord(DiskJournalRecord<V> record, byte[] entryData, OutputStream out)
            throws IOException {

        long nanoSeconds = 0;
        if (LOGGER.isDebugEnabled()) {
            nanoSeconds = System.nanoTime();
        }

        try (DataOutputStream stream = new DataOutputStream(out)) {
            int recordLength = DiskJournal.JOURNAL_RECORD_HEADER_SIZE + entryData.length;

            stream.writeInt(recordLength);
            stream.writeLong(record.getRecordId());
            stream.writeByte(record.getType());
            stream.write(entryData);
            stream.writeInt(recordLength);
        }

        LOGGER.trace("DiskJournalIOUtils::prepareBulkRecord took {}ns", (System.nanoTime() - nanoSeconds));
    }

}
