package com.github.noctarius.waljdbc.spi;

import java.io.DataOutput;

import com.github.noctarius.waljdbc.JournalEntry;

public interface JournalEntryWriter<V>
{

    void writeJournalEntry( JournalEntry<V> entry, DataOutput out );

}
