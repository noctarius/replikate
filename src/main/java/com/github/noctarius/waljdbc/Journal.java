package com.github.noctarius.waljdbc;

import com.github.noctarius.waljdbc.spi.JournalFlushedListener;

public interface Journal<V>
{

    public static final int JOURNAL_VERSION = 1;

    void appendEntry( JournalEntry<V> entry )
        throws JournalException;

    void appendEntry( JournalEntry<V> entry, JournalFlushedListener<V> listener );

    long getLastRecordId();

}
