package com.github.noctarius.replikate.spi;

import java.io.DataOutput;
import java.io.IOException;

import com.github.noctarius.replikate.JournalEntry;

public interface JournalEntryWriter<V>
{

    void writeJournalEntry( JournalEntry<V> entry, DataOutput out )
        throws IOException;

}
