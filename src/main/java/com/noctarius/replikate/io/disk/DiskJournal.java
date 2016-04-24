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

import com.noctarius.replikate.JournalBatch;
import com.noctarius.replikate.JournalEntry;
import com.noctarius.replikate.JournalListener;
import com.noctarius.replikate.JournalNamingStrategy;
import com.noctarius.replikate.JournalRecord;
import com.noctarius.replikate.exceptions.JournalException;
import com.noctarius.replikate.exceptions.SynchronousJournalException;
import com.noctarius.replikate.io.util.Tuple;
import com.noctarius.replikate.spi.AbstractJournal;
import com.noctarius.replikate.spi.JournalEntryReader;
import com.noctarius.replikate.spi.JournalEntryWriter;
import com.noctarius.replikate.spi.JournalOperation;
import com.noctarius.replikate.spi.JournalRecordIdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.noctarius.replikate.io.disk.DiskJournalIOUtils.prepareJournalEntry;

class DiskJournal<V>
        extends AbstractJournal<V> {

    static final int JOURNAL_FILE_HEADER_SIZE = 25;
    static final int JOURNAL_RECORD_HEADER_SIZE = 17;

    static final byte JOURNAL_FILE_TYPE_DEFAULT = 1;
    static final byte JOURNAL_FILE_TYPE_OVERFLOW = 2;
    static final byte JOURNAL_FILE_TYPE_BATCH = 3;

    private static final int JOURNAL_OVERFLOW_OVERHEAD_SIZE = JOURNAL_FILE_HEADER_SIZE + JOURNAL_RECORD_HEADER_SIZE;

    private static final Logger LOGGER = LoggerFactory.getLogger(DiskJournal.class);

    private final DiskJournalWriterTask diskJournalWriterTask = new DiskJournalWriterTask();
    private final BlockingQueue<JournalOperation> journalQueue = new LinkedBlockingQueue<>();
    private final Deque<DiskJournalFile<V>> journalFiles = new ConcurrentLinkedDeque<>();
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final JournalListener<V> listener;
    private final Thread diskJournalWriter;
    private final Path journalPath;
    private final int maxLogFileSize;

    DiskJournal(String name, Path journalPath, JournalListener<V> listener, int maxLogFileSize,
                JournalRecordIdGenerator recordIdGenerator, JournalEntryReader<V> reader, JournalEntryWriter<V> writer,
                JournalNamingStrategy namingStrategy, ExecutorService listenerExecutorService)
            throws IOException {

        super(name, recordIdGenerator, reader, writer, namingStrategy, listenerExecutorService);
        this.journalPath = journalPath;
        this.maxLogFileSize = maxLogFileSize;
        this.listener = listener;

        if (!Files.isDirectory(journalPath, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("journalPath is not a directory");
        }

        LOGGER.info("{}: DiskJournal starting up in {}...", getName(), journalPath.toFile().getAbsolutePath());

        boolean needsReplay = false;
        File path = journalPath.toFile();
        for (File child : path.listFiles()) {
            if (child.isDirectory()) {
                continue;
            }

            String filename = child.getName();
            if (namingStrategy.isJournal(filename)) {
                // At least one journal file is still existing so start replay
                needsReplay = true;
                break;
            }
        }

        if (needsReplay) {
            LOGGER.warn("{}: Found old journals in journaling path, starting replay...", getName());
            DiskJournalReplayer<V> replayer = new DiskJournalReplayer<>(this, listener);
            replayer.replay();
        }

        // Startup asynchronous journal writer
        diskJournalWriter = new Thread(diskJournalWriterTask, "DiskJournalWriter-" + name);
        diskJournalWriter.start();
    }

    @Override
    public void appendEntry(JournalEntry<V> entry)
            throws JournalException {

        appendEntry(entry, listener);
    }

    @Override
    public void appendEntry(JournalEntry<V> entry, JournalListener<V> listener)
            throws JournalException {

        if (shutdown.get()) {
            return;
        }

        try {
            DiskJournalEntryFacade<V> journalEntry = prepareJournalEntry(entry, getWriter());
            journalQueue.offer(new SimpleAppendOperation(journalEntry, listener));
        } catch (IOException e) {
            throw new SynchronousJournalException("Could not prepare journal entry", e);
        }
    }

    @Override
    public void appendEntrySynchronous(JournalEntry<V> entry)
            throws JournalException {

        appendEntrySynchronous(entry, listener);
    }

    @Override
    public void appendEntrySynchronous(JournalEntry<V> entry, JournalListener<V> listener)
            throws JournalException {

        if (shutdown.get()) {
            return;
        }

        appendEntry0(entry, listener);
    }

    @Override
    public JournalBatch<V> startBatchProcess() {
        return startBatchProcess(listener);
    }

    @Override
    public JournalBatch<V> startBatchProcess(JournalListener<V> listener) {
        return new DiskJournalBatchProcess<>(this, listener);
    }

    @Override
    public void close()
            throws IOException {

        if (!shutdown.compareAndSet(false, true)) {
            return;
        }

        synchronized (journalFiles) {
            try {
                diskJournalWriterTask.shutdown();

                // Wait for asynchronous journal writer to finish
                shutdownLatch.await();

                for (DiskJournalFile<V> journalFile : journalFiles) {
                    journalFile.close();
                }
            } catch (InterruptedException e) {
            }
        }
    }

    int getMaxLogFileSize() {
        return maxLogFileSize;
    }

    Path getJournalPath() {
        return journalPath;
    }

    void commitBatchProcess(final JournalBatch<V> journalBatch, final List<DiskJournalEntryFacade<V>> entries, final int dataSize,
                            final JournalListener<V> listener)
            throws JournalException {

        if (shutdown.get()) {
            throw new JournalException("DiskJournal already closed");
        }

        journalQueue.offer(new BatchCommitOperation(entries, journalBatch, dataSize, listener));
    }

    void commitBatchProcessSync(final JournalBatch<V> journalBatch, final List<DiskJournalEntryFacade<V>> entries,
                                final int dataSize, JournalListener<V> listener)
            throws JournalException {

        if (shutdown.get()) {
            throw new JournalException("DiskJournal already closed");
        }

        CountDownLatch synchronizer = new CountDownLatch(1);
        BatchCommitSyncOperation operation = newBatchCommitSyncOperation(journalBatch, entries, dataSize, listener, synchronizer);
        journalQueue.offer(operation);

        try {
            synchronizer.await();
            if (operation.getCause() != null) {
                throw operation.getCause();
            }
        } catch (InterruptedException e) {
            throw new JournalException("Wait for execution of commit was interrupted", e);
        }
    }

    void pushJournalFileFromReplay(DiskJournalFile<V> diskJournalFile) {
        synchronized (journalFiles) {
            journalFiles.push(diskJournalFile);
            setCurrentLogNumber(diskJournalFile.getLogNumber());
        }
    }

    private BatchCommitSyncOperation newBatchCommitSyncOperation(JournalBatch<V> journalBatch,
                                                                 List<DiskJournalEntryFacade<V>> entries, int dataSize,
                                                                 JournalListener<V> listener, CountDownLatch synchronizer) {

        return new BatchCommitSyncOperation(entries, journalBatch, dataSize, listener, synchronizer);
    }

    private void appendEntry0(JournalEntry<V> entry, JournalListener<V> listener) {
        try {
            synchronized (journalFiles) {
                DiskJournalFile<V> journalFile = journalFiles.peek();
                if (journalFile == null) {
                    // Replay not required or succeed so start new journal
                    journalFile = buildJournalFile();
                    journalFiles.push(journalFile);
                }

                DiskJournalEntryFacade<V> recordEntry = prepareJournalEntry(entry, getWriter());

                Tuple<DiskJournalAppendResult, JournalRecord<V>> result = journalFile.appendRecord(recordEntry);
                if (result.getLeft() == DiskJournalAppendResult.APPEND_SUCCESSFUL) {
                    if (listener != null) {
                        onCommit(listener, result.getRight());
                    }

                } else if (result.getLeft() == DiskJournalAppendResult.JOURNAL_OVERFLOW) {
                    overflowJournal(entry, listener, journalFile);

                } else if (result.getLeft() == DiskJournalAppendResult.JOURNAL_FULL_OVERFLOW) {
                    overflowLargeJournal(listener, journalFile, recordEntry);
                }
            }
        } catch (IOException e) {
            if (listener != null) {
                onFailure(listener, entry, new SynchronousJournalException("Failed to persist journal entry", e));
            }
        }
    }

    private void overflowLargeJournal(JournalListener<V> listener, DiskJournalFile<V> journalFile,
                                      DiskJournalEntryFacade<V> recordEntry)
            throws IOException {

        Tuple<DiskJournalAppendResult, JournalRecord<V>> result;
        LOGGER.debug("Record dataset too large for normal journal, using overflow journal file");

        // Close current journal file ...
        journalFile.close();

        // Calculate overflow filelength
        int length = recordEntry.cachedData.length + DiskJournal.JOURNAL_OVERFLOW_OVERHEAD_SIZE;

        // ... and start new journal ...
        journalFile = buildJournalFile(length, DiskJournal.JOURNAL_FILE_TYPE_OVERFLOW);
        journalFiles.push(journalFile);

        // ... finally retry to write to journal
        result = journalFile.appendRecord(recordEntry);
        if (result.getLeft() != DiskJournalAppendResult.APPEND_SUCCESSFUL) {
            throw new SynchronousJournalException("Overflow file could not be written");
        }

        // Notify listeners about flushed to journal
        if (listener != null) {
            onCommit(listener, result.getRight());
        }
    }

    private void overflowJournal(JournalEntry<V> entry, JournalListener<V> listener, DiskJournalFile<V> journalFile)
            throws IOException {

        LOGGER.debug("Journal full, overflowing to next one...");

        // Close current journal file ...
        journalFile.close();

        // ... and start new journal ...
        journalFiles.push(buildJournalFile());

        // ... finally retry to write to journal
        appendEntrySynchronous(entry, listener);
    }

    private DiskJournalFile<V> buildJournalFile()
            throws IOException {

        return buildJournalFile(getMaxLogFileSize(), DiskJournal.JOURNAL_FILE_TYPE_DEFAULT);
    }

    private DiskJournalFile<V> buildJournalFile(int maxLogFileSize, byte type)
            throws IOException {

        long logNumber = nextLogNumber();
        String filename = getNamingStrategy().generate(logNumber);
        File journalFile = new File(journalPath.toFile(), filename);
        return new DiskJournalFile<>(this, journalFile, logNumber, maxLogFileSize, type);
    }

    private class DiskJournalWriterTask
            implements Runnable {

        private final AtomicBoolean shutdown = new AtomicBoolean(false);

        @Override
        public void run() {
            try {
                while (true) {
                    try {
                        // If all work is done, break up
                        if (shutdown.get() && journalQueue.size() == 0) {
                            break;
                        }

                        JournalOperation operation = journalQueue.take();
                        if (operation != null) {
                            operation.execute();
                        }

                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        if (!shutdown.get()) {
                            LOGGER.warn("DiskJournalWriter ignores to interrupt, to shutdown "
                                    + "it call DiskJournalWriterTask::shutdown()", e);
                        }
                    }
                }
            } finally {
                shutdownLatch.countDown();
            }
        }

        void shutdown() {
            shutdown.compareAndSet(false, true);
            diskJournalWriter.interrupt();
        }
    }

    private class BatchCommitOperation
            implements JournalOperation {

        private final List<DiskJournalEntryFacade<V>> entries;

        private final JournalBatch<V> journalBatch;

        private final JournalListener<V> listener;

        private final int dataSize;

        private BatchCommitOperation(List<DiskJournalEntryFacade<V>> entries, JournalBatch<V> journalBatch, int dataSize,
                                     JournalListener<V> listener) {

            this.entries = entries;
            this.journalBatch = journalBatch;
            this.listener = listener;
            this.dataSize = dataSize;
        }

        public void execute() {
            synchronized (journalFiles) {
                // Storing current recordId for case of rollback
                long markedRecordId = getRecordIdGenerator().lastGeneratedRecordId();

                try {
                    commit();
                } catch (Exception e) {
                    if (listener != null) {
                        onFailure(listener, journalBatch,
                                new SynchronousJournalException("Failed to persist journal batch process", e));
                    }

                    // Rollback the journal file
                    rollback();

                    // Rollback the recordId
                    getRecordIdGenerator().notifyHighestJournalRecordId(markedRecordId);
                }
            }
        }

        void rollback() {
            DiskJournalFile<V> journalFile = journalFiles.pop();
            try {
                if (journalFile.getHeader().getType() == JOURNAL_FILE_TYPE_BATCH) {
                    journalFile.close();
                    String fileName = journalFile.getFileName();
                    Files.delete(journalPath.resolve(fileName));
                }
            } catch (IOException ioe) {
                throw new JournalException("Could not rollback journal batch file", ioe);
            }
        }

        void commit()
                throws IOException {

            // Calculate the file size of the batch journal file ...
            int calculatedDataSize = dataSize + (entries.size() * DiskJournal.JOURNAL_RECORD_HEADER_SIZE);
            int calculatedLogFileSize = calculatedDataSize + DiskJournal.JOURNAL_FILE_HEADER_SIZE;

            // ... and start new journal
            DiskJournalFile<V> journalFile = buildJournalFile(calculatedLogFileSize, JOURNAL_FILE_TYPE_BATCH);
            journalFiles.push(journalFile);

            // Persist all entries to disk ...
            Tuple<DiskJournalAppendResult, List<JournalRecord<V>>> result = journalFile
                    .appendRecords(entries, calculatedDataSize);

            if (result.getLeft() != DiskJournalAppendResult.APPEND_SUCCESSFUL) {
                throw new SynchronousJournalException("Failed to persist journal entry");
            }

            // ... and if non of them failed just announce them as committed
            for (JournalRecord<V> record : result.getRight()) {
                onCommit(listener, record);
            }
        }
    }

    private class BatchCommitSyncOperation
            extends BatchCommitOperation {

        private final CountDownLatch synchronizer;

        private volatile JournalException journalException = null;

        BatchCommitSyncOperation(List<DiskJournalEntryFacade<V>> entries, JournalBatch<V> journalBatch, int dataSize,
                                 JournalListener<V> listener, CountDownLatch synchronizer) {

            super(entries, journalBatch, dataSize, listener);
            this.synchronizer = synchronizer;
        }

        @Override
        public void execute() {
            synchronized (journalFiles) {
                // Storing current recordId for case of rollback
                long markedRecordId = getRecordIdGenerator().lastGeneratedRecordId();

                try {
                    commit();
                } catch (Exception e) {
                    journalException = new JournalException("Cannot rollback journal batch file", e);

                    // Rollback the journal file
                    rollback();

                    // Rollback the recordId
                    getRecordIdGenerator().notifyHighestJournalRecordId(markedRecordId);
                } finally {
                    synchronizer.countDown();
                }
            }
        }

        JournalException getCause() {
            return journalException;
        }
    }

}
