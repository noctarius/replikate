package com.github.noctarius.replikate.spi;

import java.io.IOException;

import com.github.noctarius.replikate.JournalEntry;

public interface JournalEntryReader<V>
{

    JournalEntry<V> readJournalEntry( long recordId, byte type, byte[] data )
        throws IOException;

}
