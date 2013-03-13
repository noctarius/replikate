package com.github.noctarius.waljdbc.io.disk;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.github.noctarius.waljdbc.Journal;
import com.github.noctarius.waljdbc.JournalEntry;
import com.github.noctarius.waljdbc.JournalException;

class DiskJournalFile<V>
{

    private final RandomAccessFile raf;

    private final Lock appendLock = new ReentrantLock();

    private final JournalFileHeader header;

    private final DiskJournal<V> journal;

    public DiskJournalFile( DiskJournal<V> journal, File file, long logNumber )
        throws IOException
    {
        this( journal, file, logNumber, true );
    }

    DiskJournalFile( DiskJournal<V> journal, File file, long logNumber, boolean createIfNotExisting )
        throws IOException
    {
        if ( !file.exists() && !createIfNotExisting )
        {
            throw new JournalException( "File " + file.getAbsolutePath() + " does not exists and creation is forbidden" );
        }

        this.journal = journal;
        this.raf = new RandomAccessFile( file, "rws" );
        this.header = createJournal( buildHeader( journal.getMaxLogFileSize(), logNumber ) );
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

    public DiskJournalAppendResult appendRecord( JournalEntry<V> entry )
        throws IOException
    {
        try
        {
            appendLock.lock();
            try ( ByteArrayOutputStream out = new ByteArrayOutputStream( 100 );
                            DataOutputStream stream = new DataOutputStream( out ) )
            {
                byte[] entryData = null;
                if ( entry instanceof DiskJournalEntry )
                {
                    DiskJournalEntry<V> journalEntry = (DiskJournalEntry<V>) entry;
                    if ( journalEntry.cachedData != null )
                    {
                        entryData = journalEntry.cachedData;
                    }
                }

                if ( entryData == null )
                {
                    journal.getWriter().writeJournalEntry( entry, stream );
                    entryData = out.toByteArray();

                    if ( entry instanceof DiskJournalEntry )
                    {
                        ( (DiskJournalEntry<V>) entry ).cachedData = entryData;
                    }
                }

                int length = entryData.length + DiskJournal.JOURNAL_RECORD_HEADER_SIZE;
                if ( length > header.getMaxLogFileSize() )
                {
                    return DiskJournalAppendResult.JOURNAL_FULL_OVERFLOW;
                }
                else if ( header.getMaxLogFileSize() < getPosition() + length )
                {
                    return DiskJournalAppendResult.JOURNAL_OVERFLOW;
                }

                long recordId = journal.getRecordIdGenerator().nextRecordId();
                DiskJournalRecord<V> record = new DiskJournalRecord<V>( entry, recordId );
                DiskJournalRecord.writeRecord( record, entryData, raf );

                return DiskJournalAppendResult.APPEND_SUCCESSFUL;
            }
        }
        finally
        {
            appendLock.unlock();
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

    private JournalFileHeader createJournal( JournalFileHeader header )
        throws IOException
    {
        byte[] prefiller = new byte[header.getMaxLogFileSize()];
        Arrays.fill( prefiller, (byte) 0 );

        // Resize the file to maxLogFileSize
        raf.setLength( header.getMaxLogFileSize() );
        raf.seek( 0 );
        raf.write( prefiller );

        raf.seek( 0 );
        raf.write( JournalFileHeader.MAGIC_NUMBER );
        raf.writeInt( header.getVersion() );
        raf.writeInt( header.getMaxLogFileSize() );
        raf.writeLong( header.getLogNumber() );
        raf.write( header.getType() );
        raf.writeInt( header.getFirstDataOffset() );

        return header;
    }

    private JournalFileHeader buildHeader( int maxLogFileSize, long logNumber )
    {
        return new JournalFileHeader( Journal.JOURNAL_VERSION, maxLogFileSize, logNumber,
                                      DiskJournal.JOURNAL_FILE_TYPE_DEFAULT );
    }

}
