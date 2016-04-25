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

import com.noctarius.replikate.JournalEntry;
import com.noctarius.replikate.JournalListener;
import com.noctarius.replikate.JournalRecord;
import com.noctarius.replikate.exceptions.ReplayCancellationException;
import com.noctarius.replikate.impl.util.Tuple;
import com.noctarius.replikate.spi.JournalEntryReader;
import com.noctarius.replikate.spi.ReplayNotificationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.noctarius.replikate.impl.disk.DiskJournalIOUtils.readHeader;

class DiskJournalReplayer<V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DiskJournalReplayer.class);

    private final DiskJournal<V> journal;

    private final JournalListener<V> listener;

    DiskJournalReplayer(DiskJournal<V> journal, JournalListener<V> listener) {
        this.journal = journal;
        this.listener = listener;
    }

    void replay() {
        List<DiskJournalRecord<V>> records = new LinkedList<>();
        List<DiskJournalFile<V>> diskJournalFiles = new LinkedList<>();

        File directory = journal.getJournalPath().toFile();
        Stream.of(directory.listFiles()).forEach(collectJournalFiles(records, diskJournalFiles));

        Collections.sort(diskJournalFiles);
        Collections.sort(records);

        // Iterate the list and search for holes in history
        searchSuspiciousRecords(records);

        // Push all found journal files to DiskJournal
        diskJournalFiles.forEach(journal::pushJournalFileFromReplay);

        // Replay records
        for (DiskJournalRecord<V> record : records) {
            LOGGER.info("{}: Re-announcing journal entry  {}", journal.getName(), record.getRecordId());
            try {
                ReplayNotificationResult result = listener.onReplayRecordId(journal, record);
                if (result == ReplayNotificationResult.Except) {
                    throw new ReplayCancellationException("Replay of journal was aborted by callback");
                } else if (result == ReplayNotificationResult.Terminate) {
                    journal.getRecordIdGenerator().notifyHighestJournalRecordId(record.getRecordId());
                    break;
                }
                journal.getRecordIdGenerator().notifyHighestJournalRecordId(record.getRecordId());
            } catch (RuntimeException e) {
                throw new ReplayCancellationException("Replay of journal was aborted due to exception in callback", e);
            }
        }
    }

    private void searchSuspiciousRecords(List<DiskJournalRecord<V>> records) {
        long lastRecordId = -1;
        JournalRecord<V> lastRecord = null;
        for (DiskJournalRecord<V> record : records) {
            if (lastRecordId != -1 && record.getRecordId() != lastRecordId + 1) {
                LOGGER.error("{}: There is a hole in history in journal {}->{}", journal.getName(), lastRecordId,
                        record.getRecordId());

                if (replaySuspiciousRecord(lastRecord, record)) {
                    break;
                }
            }
            lastRecordId = record.getRecordId();
            lastRecord = record;
        }
    }

    private Consumer<File> collectJournalFiles(List<DiskJournalRecord<V>> records, List<DiskJournalFile<V>> diskJournalFiles) {
        return (child) -> {
            if (!child.isDirectory()) {
                String filename = child.getName();
                if (journal.getNamingStrategy().isJournal(filename)) {
                    Tuple<DiskJournalFile<V>, List<DiskJournalRecord<V>>> result = readForward(child.toPath());
                    diskJournalFiles.add(result.getLeft());
                    records.addAll(result.getRight());
                }
            }
        };
    }

    private boolean replaySuspiciousRecord(JournalRecord<V> lastRecord, DiskJournalRecord<V> record) {
        try {
            ReplayNotificationResult result = listener.onReplaySuspiciousRecordId(journal, lastRecord, record);
            if (result == ReplayNotificationResult.Except) {
                throw new ReplayCancellationException("Replay of journal was aborted by callback");
            } else if (result == ReplayNotificationResult.Terminate) {
                return true;
            }
        } catch (RuntimeException e) {
            throw new ReplayCancellationException(
                    "Replay of journal was aborted due " + "to missing recordId in the journal file", e);
        }
        return false;
    }

    private Tuple<DiskJournalFile<V>, List<DiskJournalRecord<V>>> readForward(Path file) {
        DiskJournalFile<V> diskJournalFile = null;
        List<DiskJournalRecord<V>> records = new LinkedList<>();
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            DiskJournalFileHeader header = readHeader(raf);
            diskJournalFile = new DiskJournalFile<>(raf, file.toFile().getName(), header, journal);
            LOGGER.info("{}: Reading old journal file with logNumber {}", journal.getName(), header.getLogNumber());

            int pos = header.getFirstDataOffset();
            while (pos < raf.length()) {
                // Read length at begin of the record start
                raf.seek(pos);
                int startingLength = raf.readInt();

                if (startingLength == 0) {
                    // File is completely read
                    break;
                }

                // Read length at begin of the record end
                raf.seek(pos + startingLength - 4);
                int endingLength = raf.readInt();

                // If both length values differ this record is broken
                if (startingLength != endingLength) {
                    LOGGER.debug("pos={}, startingLength={}, endingLength={}", pos, startingLength, endingLength);
                    LOGGER.info("{}: Incomplete record in journal file with logNumber {}", journal.getName(),
                            header.getLogNumber());
                    break;
                }

                LOGGER.debug("{}: Found new record in logNumber {}", journal.getName(), header.getLogNumber());

                // Read recordId
                raf.seek(pos + 4);
                long recordId = raf.readLong();

                LOGGER.debug("{}: Reading record {}", journal.getName(), recordId);

                // Read information of the entry
                byte type = raf.readByte();
                int entryLength = startingLength - DiskJournal.JOURNAL_RECORD_HEADER_SIZE;
                byte[] entryData = new byte[entryLength];
                raf.readFully(entryData);

                JournalEntryReader<V> reader = journal.getReader();
                JournalEntry<V> journalEntry = reader.readJournalEntry(recordId, type, entryData);
                records.add(new DiskJournalRecord<>(journalEntry, recordId));

                pos += startingLength;
            }
        } catch (IOException e) {
            // Something went wrong but we want to execute as much journal entries as possible so we'll ignore that one
            // here!
        }
        return new Tuple<>(diskJournalFile, records);
    }

}
