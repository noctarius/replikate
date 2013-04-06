package com.github.noctarius.replikate.spi;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.github.noctarius.replikate.Journal;
import com.github.noctarius.replikate.JournalBatch;
import com.github.noctarius.replikate.JournalEntry;
import com.github.noctarius.replikate.JournalListener;
import com.github.noctarius.replikate.JournalNamingStrategy;
import com.github.noctarius.replikate.JournalRecord;
import com.github.noctarius.replikate.exceptions.JournalException;

public abstract class AbstractJournal<V>
    implements Journal<V>
{

    private final int journalVersion = JOURNAL_VERSION;

    private final ExecutorService listenerExecutorService =
        Executors.newFixedThreadPool( 5, new NamedThreadFactory( "RepliKate-Listener" ) );

    private final AtomicLong logNumber = new AtomicLong( 0 );

    private final JournalRecordIdGenerator recordIdGenerator;

    private final JournalEntryReader<V> reader;

    private final JournalEntryWriter<V> writer;

    private final JournalNamingStrategy namingStrategy;

    private final int maxLogFileSize;

    private final String name;

    protected AbstractJournal( String name, int maxLogFileSize, JournalRecordIdGenerator recordIdGenerator,
                               JournalEntryReader<V> reader, JournalEntryWriter<V> writer,
                               JournalNamingStrategy namingStrategy )
    {
        this.name = name;
        this.maxLogFileSize = maxLogFileSize;
        this.recordIdGenerator = recordIdGenerator;
        this.reader = reader;
        this.writer = writer;
        this.namingStrategy = namingStrategy;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public long nextLogNumber()
    {
        return logNumber.incrementAndGet();
    }

    @Override
    public JournalRecordIdGenerator getRecordIdGenerator()
    {
        return recordIdGenerator;
    }

    @Override
    public JournalEntryReader<V> getReader()
    {
        return reader;
    }

    @Override
    public JournalEntryWriter<V> getWriter()
    {
        return writer;
    }

    @Override
    public JournalNamingStrategy getNamingStrategy()
    {
        return namingStrategy;
    }

    @Override
    public void close()
        throws IOException
    {
        listenerExecutorService.shutdown();
    }

    public int getJournalVersion()
    {
        return journalVersion;
    }

    public int getMaxLogFileSize()
    {
        return maxLogFileSize;
    }

    protected void setCurrentLogNumber( long logNumber )
    {
        this.logNumber.set( logNumber );
    }

    protected void onCommit( final JournalListener<V> journalListener, final JournalRecord<V> record )
    {
        listenerExecutorService.execute( new Runnable()
        {

            @Override
            public void run()
            {
                journalListener.onCommit( record );
            }
        } );
    }

    protected void onFailure( final JournalListener<V> journalListener, final JournalEntry<V> entry,
                              final JournalException cause )
    {
        listenerExecutorService.execute( new Runnable()
        {

            @Override
            public void run()
            {
                journalListener.onFailure( entry, cause );
            }
        } );
    }

    protected void onFailure( final JournalListener<V> journalListener, final JournalBatch<V> journalBatch,
                              final JournalException cause )
    {
        listenerExecutorService.execute( new Runnable()
        {

            @Override
            public void run()
            {
                journalListener.onFailure( journalBatch, cause );
            }
        } );
    }

}
