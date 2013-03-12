package com.github.noctarius.waljdbc.spi;

import com.github.noctarius.waljdbc.Journal;
import com.github.noctarius.waljdbc.JournalEntry;

public abstract class AbstractJournal<V>
    implements Journal<V>
{

    private final int journalVersion = JOURNAL_VERSION;

    private final JournalRecordIdGenerator recordIdGenerator;

    private final JournalEntryReader<V> reader;

    private final JournalEntryWriter<V> writer;

    private final int maxLogFileSize;

    protected AbstractJournal( int maxLogFileSize, JournalRecordIdGenerator recordIdGenerator,
                               JournalEntryReader<V> reader, JournalEntryWriter<V> writer )
    {
        this.maxLogFileSize = maxLogFileSize;
        this.recordIdGenerator = recordIdGenerator;
        this.reader = reader;
        this.writer = writer;
    }

    public void appendEntry( JournalEntry<V> entry )
    {
        appendEntry( entry, null );
    }

    public int getJournalVersion()
    {
        return journalVersion;
    }

    public JournalRecordIdGenerator getRecordIdGenerator()
    {
        return recordIdGenerator;
    }

    public JournalEntryReader<V> getReader()
    {
        return reader;
    }

    public JournalEntryWriter<V> getWriter()
    {
        return writer;
    }

    public int getMaxLogFileSize()
    {
        return maxLogFileSize;
    }

}
