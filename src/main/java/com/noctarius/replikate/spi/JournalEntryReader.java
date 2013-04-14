package com.noctarius.replikate.spi;

import java.io.IOException;

import com.noctarius.replikate.JournalEntry;

public interface JournalEntryReader<V>
{

    JournalEntry<V> readJournalEntry( long recordId, byte type, byte[] data )
        throws IOException;

}
