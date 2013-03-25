package com.github.noctarius.waljdbc.io.disk;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.noctarius.waljdbc.Journal;
import com.github.noctarius.waljdbc.JournalRecord;
import com.github.noctarius.waljdbc.exceptions.JournalException;

class DiskJournalFile<V>
{

    private final Logger LOGGER = LoggerFactory.getLogger( DiskJournalFile.class );

    private final RandomAccessFile raf;

    private final Lock appendLock = new ReentrantLock();

    private final JournalFileHeader header;

    private final DiskJournal<V> journal;

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
        this.raf = new RandomAccessFile( file, "rws" );
        this.header = DiskJournalIOUtils.createJournal( raf, buildHeader( maxLogFileSize, type, logNumber ) );
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
            try ( ByteArrayOutputStream out = new ByteArrayOutputStream( 100 );
                            DataOutputStream stream = new DataOutputStream( out ) )
            {
                byte[] entryData = null;
                if ( entry.cachedData != null )
                {
                    entryData = entry.cachedData;
                }

                if ( entryData == null )
                {
                    journal.getWriter().writeJournalEntry( entry, stream );
                    entryData = out.toByteArray();
                    entry.cachedData = entryData;
                }

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
        }
        finally
        {
            appendLock.unlock();
            LOGGER.trace( "DiskJournalFile::appendRecord took {}ns", ( System.nanoTime() - nanoSeconds ) );
        }
    }

    public void close()
        throws IOException
    {
        raf.close();
    }

    public JournalFileHeader getHeader()
    {
        return header;
    }

    private JournalFileHeader buildHeader( int maxLogFileSize, byte type, long logNumber )
    {
        return new JournalFileHeader( Journal.JOURNAL_VERSION, maxLogFileSize, logNumber, type );
    }

}
