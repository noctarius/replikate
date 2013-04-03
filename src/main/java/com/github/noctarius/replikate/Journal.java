package com.github.noctarius.replikate;

import java.io.IOException;

import com.github.noctarius.replikate.exceptions.JournalException;
import com.github.noctarius.replikate.spi.JournalEntryReader;
import com.github.noctarius.replikate.spi.JournalEntryWriter;
import com.github.noctarius.replikate.spi.JournalRecordIdGenerator;

public interface Journal<V>
{

    public static final int JOURNAL_VERSION = 1;

    String getName();

    void appendEntry( JournalEntry<V> entry )
        throws JournalException;

    void appendEntry( JournalEntry<V> entry, JournalListener<V> listener );

    JournalBatch<V> startBatchProcess();

    JournalBatch<V> startBatchProcess( JournalListener<V> listener );

    long getLastRecordId();

    long nextLogNumber();

    void close()
        throws IOException;

    JournalRecordIdGenerator getRecordIdGenerator();

    JournalEntryReader<V> getReader();

    JournalEntryWriter<V> getWriter();

    JournalNamingStrategy getNamingStrategy();

}
