package com.github.noctarius.waljdbc;

public abstract class AbstractJournal<V>
    implements Journal<V>
{

    private final int journalVersion = JOURNAL_VERSION;

    private final int maxLogFileSize;

    protected AbstractJournal( int maxLogFileSize )
    {
        this.maxLogFileSize = maxLogFileSize;
    }

    public void appendEntry( JournalEntry<V> entry )
    {
        appendEntry( entry, null );
    }

}
