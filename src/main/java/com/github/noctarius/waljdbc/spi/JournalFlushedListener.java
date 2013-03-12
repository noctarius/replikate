package com.github.noctarius.waljdbc.spi;

import com.github.noctarius.waljdbc.JournalEntry;
import com.github.noctarius.waljdbc.JournalException;

public interface JournalFlushedListener<V>
{

    void flushed( JournalEntry<V> entry );

    void failed( JournalEntry<V> entry, JournalException cause );

}
