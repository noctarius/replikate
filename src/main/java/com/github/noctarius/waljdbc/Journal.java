package com.github.noctarius.waljdbc;

import java.io.IOException;

import com.github.noctarius.waljdbc.exceptions.JournalException;
import com.github.noctarius.waljdbc.spi.JournalEntryReader;
import com.github.noctarius.waljdbc.spi.JournalEntryWriter;
import com.github.noctarius.waljdbc.spi.JournalListener;
import com.github.noctarius.waljdbc.spi.JournalNamingStrategy;
import com.github.noctarius.waljdbc.spi.JournalRecordIdGenerator;

public interface Journal<V>
{

    public static final int JOURNAL_VERSION = 1;

    String getName();

    void appendEntry( JournalEntry<V> entry )
        throws JournalException;

    void appendEntry( JournalEntry<V> entry, JournalListener<V> listener );

    long getLastRecordId();

    long nextLogNumber();

    void close()
        throws IOException;

    JournalRecordIdGenerator getRecordIdGenerator();

    JournalEntryReader<V> getReader();

    JournalEntryWriter<V> getWriter();

    JournalNamingStrategy getNamingStrategy();

}
