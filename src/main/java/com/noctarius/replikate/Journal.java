package com.noctarius.replikate;

import java.io.IOException;

import com.noctarius.replikate.exceptions.JournalException;
import com.noctarius.replikate.spi.JournalEntryReader;
import com.noctarius.replikate.spi.JournalEntryWriter;
import com.noctarius.replikate.spi.JournalRecordIdGenerator;

public interface Journal<V>
{

    public static final int JOURNAL_VERSION = 1;

    String getName();

    void appendEntry( JournalEntry<V> entry )
        throws JournalException;

    void appendEntry( JournalEntry<V> entry, JournalListener<V> listener )
        throws JournalException;

    void appendEntrySynchronous( JournalEntry<V> entry )
        throws JournalException;

    void appendEntrySynchronous( JournalEntry<V> entry, JournalListener<V> listener )
        throws JournalException;

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
