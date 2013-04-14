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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.noctarius.replikate.Journal;
import com.noctarius.replikate.JournalRecord;
import com.noctarius.replikate.exceptions.JournalException;
import com.noctarius.replikate.io.util.ByteArrayBufferOutputStream;
import com.noctarius.replikate.io.util.Tuple;

class DiskJournalFile<V>
    implements Comparable<DiskJournalFile<V>>
{

    private final Logger LOGGER = LoggerFactory.getLogger( DiskJournalFile.class );

    private final RandomAccessFile raf;

    private final Lock appendLock = new ReentrantLock();

    private final DiskJournalFileHeader header;

    private final DiskJournal<V> journal;

    private final String fileName;

    public DiskJournalFile( DiskJournal<V> journal, File file, long logNumber, int maxLogFileSize )
        throws IOException
    {
        this( journal, file, logNumber, maxLogFileSize, DiskJournal.JOURNAL_FILE_TYPE_DEFAULT );
    }

    public DiskJournalFile( DiskJournal<V> journal, File file, long logNumber, int maxLogFileSize, byte type )
        throws IOException
    {
        this( journal, file, logNumber, maxLogFileSize, type, true );
    }

    DiskJournalFile( DiskJournal<V> journal, File file, long logNumber, int maxLogFileSize, byte type,
                     boolean createIfNotExisting )
        throws IOException
    {
        if ( !file.exists() && !createIfNotExisting )
        {
            throw new JournalException( "File " + file.getAbsolutePath() + " does not exists and creation is forbidden" );
        }

        this.journal = journal;
        this.fileName = file.getName();
        this.raf = new RandomAccessFile( file, "rws" );
        this.header = DiskJournalIOUtils.createJournal( raf, buildHeader( maxLogFileSize, type, logNumber ) );
    }

    public DiskJournalFile( RandomAccessFile raf, String fileName, DiskJournalFileHeader header, DiskJournal<V> journal )
    {
        this.raf = raf;
        this.fileName = fileName;
        this.header = header;
        this.journal = journal;
    }

    public String getFileName()
    {
        return fileName;
    }

    public int getPosition()
    {
        try
        {
            return (int) raf.getFilePointer();
        }
        catch ( IOException e )
        {
            return -1;
        }
    }

    public Tuple<DiskJournalAppendResult, JournalRecord<V>> appendRecord( DiskJournalEntryFacade<V> entry )
        throws IOException
    {
        long nanoSeconds = System.nanoTime();

        try
        {
            appendLock.lock();

            byte[] entryData = entry.cachedData;
            int length = entryData.length + DiskJournal.JOURNAL_RECORD_HEADER_SIZE;
            if ( length > header.getMaxLogFileSize() )
            {
                return new Tuple<>( DiskJournalAppendResult.JOURNAL_FULL_OVERFLOW, null );
            }
            else if ( header.getMaxLogFileSize() < getPosition() + length )
            {
                return new Tuple<>( DiskJournalAppendResult.JOURNAL_OVERFLOW, null );
            }

            long recordId = journal.getRecordIdGenerator().nextRecordId();
            DiskJournalRecord<V> record = new DiskJournalRecord<V>( entry.wrappedEntry, recordId );
            DiskJournalIOUtils.writeRecord( record, entryData, raf );

            return new Tuple<>( DiskJournalAppendResult.APPEND_SUCCESSFUL, (JournalRecord<V>) record );
        }
        finally
        {
            appendLock.unlock();
            LOGGER.trace( "DiskJournalFile::appendRecord took {}ns", ( System.nanoTime() - nanoSeconds ) );
        }
    }

    public Tuple<DiskJournalAppendResult, List<JournalRecord<V>>> appendRecords( List<DiskJournalEntryFacade<V>> entries,
                                                                                 int dataSize )
        throws IOException
    {
        long nanoSeconds = System.nanoTime();

        try
        {
            appendLock.lock();

            byte[] data = new byte[dataSize];
            List<JournalRecord<V>> records = new LinkedList<>();
            try ( ByteArrayBufferOutputStream out = new ByteArrayBufferOutputStream( data ) )
            {
                for ( DiskJournalEntryFacade<V> entry : entries )
                {
                    long recordId = journal.getRecordIdGenerator().nextRecordId();
                    DiskJournalRecord<V> record = new DiskJournalRecord<V>( entry.wrappedEntry, recordId );
                    DiskJournalIOUtils.prepareBulkRecord( record, entry.cachedData, out );
                    records.add( record );
                }

                raf.write( data );
            }
            return new Tuple<>( DiskJournalAppendResult.APPEND_SUCCESSFUL, records );
        }
        finally
        {
            appendLock.unlock();
            LOGGER.trace( "DiskJournalFile::appendRecords took {}ns", ( System.nanoTime() - nanoSeconds ) );
        }
    }

    public void close()
        throws IOException
    {
        raf.close();
    }

    public DiskJournalFileHeader getHeader()
    {
        return header;
    }

    public long getLogNumber()
    {
        return header.getLogNumber();
    }

    @Override
    public int compareTo( DiskJournalFile<V> o )
    {
        return Long.valueOf( header.getLogNumber() ).compareTo( o.getLogNumber() );
    }

    private DiskJournalFileHeader buildHeader( int maxLogFileSize, byte type, long logNumber )
    {
        return new DiskJournalFileHeader( Journal.JOURNAL_VERSION, maxLogFileSize, logNumber, type );
    }

}
