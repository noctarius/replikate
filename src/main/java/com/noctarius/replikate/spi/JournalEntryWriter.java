package com.noctarius.replikate.spi;

import java.io.DataOutput;
import java.io.IOException;

import com.noctarius.replikate.JournalEntry;

public interface JournalEntryWriter<V>
{

    void writeJournalEntry( JournalEntry<V> entry, DataOutput out )
        throws IOException;

    int estimateRecordSize( JournalEntry<V> entry );

    boolean isRecordSizeEstimatable();

}
