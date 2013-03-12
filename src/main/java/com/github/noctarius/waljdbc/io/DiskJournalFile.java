package com.github.noctarius.waljdbc.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.github.noctarius.waljdbc.Journal;
import com.github.noctarius.waljdbc.JournalEntry;
import com.github.noctarius.waljdbc.JournalException;

public class DiskJournalFile<V>
{

    private final RandomAccessFile raf;

    private final Lock appendLock = new ReentrantLock();

    private final JournalFileHeader header;

    private final DiskJournal<V> journal;

    public DiskJournalFile( DiskJournal<V> journal, File file )
        throws IOException
    {
        this( journal, file, true );
    }

    public DiskJournalFile( DiskJournal<V> journal, File file, boolean createIfNotExisting )
        throws IOException
    {
        if ( !file.exists() && !createIfNotExisting )
        {
            throw new JournalException( "File " + file.getAbsolutePath() + " does not exists and creation is forbidden" );
        }

        this.journal = journal;
        this.raf = new RandomAccessFile( file, "rws" );
        this.header = file.length() > 0 ? readHeader() : createJournal( buildHeader( journal.getMaxLogFileSize() ) );
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

            int length = entry.getLength() + DiskJournal.JOURNAL_RECORD_HEADER_SIZE;
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
            DiskJournalRecord.writeRecord( record, journal.getWriter(), raf );

            return DiskJournalAppendResult.APPEND_SUCCESSFUL;
        }
        finally
        {
            appendLock.unlock();
        }
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
        raf.writeInt( header.getVersion() );
        raf.writeInt( header.getMaxLogFileSize() );
        raf.writeLong( header.getLogFileNumber() );
        raf.write( header.getType() );
        raf.writeInt( header.getFirstDataOffset() );

        return header;
    }

    private JournalFileHeader readHeader()
        throws IOException
    {
        long filePointer = raf.getFilePointer();
        raf.seek( 0 );
        int version = raf.readInt();
        int maxLogFileSize = raf.readInt();
        long logFileNumber = raf.readLong();
        byte type = raf.readByte();
        int firstDataOffset = raf.readInt();
        raf.seek( filePointer );
        return new JournalFileHeader( version, maxLogFileSize, logFileNumber, type, firstDataOffset );
    }

    private JournalFileHeader buildHeader( int maxLogFileSize )
    {
        return new JournalFileHeader( Journal.JOURNAL_VERSION, maxLogFileSize, journal.nextLogFileNumber(),
                                      DiskJournal.JOURNAL_FILE_TYPE_DEFAULT );
    }

}
