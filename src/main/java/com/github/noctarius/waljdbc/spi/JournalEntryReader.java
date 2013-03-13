package com.github.noctarius.waljdbc.spi;

import java.io.IOException;

import com.github.noctarius.waljdbc.JournalEntry;

public interface JournalEntryReader<V>
{

    JournalEntry<V> readJournalEntry( long recordId, byte type, byte[] data )
        throws IOException;

}
