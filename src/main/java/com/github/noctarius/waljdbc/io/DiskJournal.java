package com.github.noctarius.waljdbc.io;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.github.noctarius.waljdbc.JournalEntry;
import com.github.noctarius.waljdbc.JournalException;
import com.github.noctarius.waljdbc.spi.AbstractJournal;
import com.github.noctarius.waljdbc.spi.JournalEntryReader;
import com.github.noctarius.waljdbc.spi.JournalEntryWriter;
import com.github.noctarius.waljdbc.spi.JournalFlushedListener;
import com.github.noctarius.waljdbc.spi.JournalRecordIdGenerator;

public class DiskJournal<V>
    extends AbstractJournal<V>
{

    public static final int JOURNAL_FILE_HEADER_SIZE = 32;

    public static final int JOURNAL_RECORD_HEADER_SIZE = 17;

    public static final byte JOURNAL_FILE_TYPE_DEFAULT = 1;

    public static final byte JOURNAL_FILE_TYPE_OVERFLOW = 2;

    private final AtomicReference<DiskJournalFile<V>> journalFile = new AtomicReference<>();

    private final AtomicLong logFileNumber = new AtomicLong( 0 );

    public DiskJournal( int maxLogFileSize, JournalRecordIdGenerator recordIdGenerator, JournalEntryReader<V> reader,
                        JournalEntryWriter<V> writer )
    {
        super( maxLogFileSize, recordIdGenerator, reader, writer );
    }

    @Override
    public void appendEntry( JournalEntry<V> entry, JournalFlushedListener<V> listener )
    {
        try
        {
            journalFile.get().appendRecord( entry );
            listener.flushed( entry );
        }
        catch ( IOException e )
        {
            listener.failed( entry, new JournalException( "Failed to persist journal entry", e ) );
        }
    }

    @Override
    public long getLastRecordId()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    long nextLogFileNumber()
    {
        return logFileNumber.incrementAndGet();
    }

}
