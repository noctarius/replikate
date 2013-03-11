package com.github.noctarius.waljdbc;

public interface Journal<V>
{

    public static final int JOURNAL_VERSION = 1;

    void appendEntry( JournalEntry<V> entry )
        throws JournalException;

    void appendEntry( JournalEntry<V> entry, JournalFlushedListener<V> listener );

}
