package com.github.noctarius.replikate.spi;

import java.util.concurrent.atomic.AtomicLong;

import com.github.noctarius.replikate.Journal;
import com.github.noctarius.replikate.JournalNamingStrategy;

public abstract class AbstractJournal<V>
    implements Journal<V>
{

    private final int journalVersion = JOURNAL_VERSION;

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

}
