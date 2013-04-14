package com.noctarius.replikate;

import com.noctarius.replikate.spi.JournalEntryReader;
import com.noctarius.replikate.spi.JournalEntryWriter;
import com.noctarius.replikate.spi.JournalRecordIdGenerator;

public class JournalConfiguration<V>
{

    private JournalListener<V> listener;

    private JournalRecordIdGenerator recordIdGenerator;

    private JournalNamingStrategy namingStrategy;

    private JournalEntryReader<V> entryReader;

    private JournalEntryWriter<V> entryWriter;

    public JournalListener<V> getListener()
    {
        return listener;
    }

    public void setListener( JournalListener<V> listener )
    {
        this.listener = listener;
    }

    public JournalRecordIdGenerator getRecordIdGenerator()
    {
        return recordIdGenerator;
    }

    public void setRecordIdGenerator( JournalRecordIdGenerator recordIdGenerator )
    {
        this.recordIdGenerator = recordIdGenerator;
    }

    public JournalNamingStrategy getNamingStrategy()
    {
        return namingStrategy;
    }

    public void setNamingStrategy( JournalNamingStrategy namingStrategy )
    {
        this.namingStrategy = namingStrategy;
    }

    public JournalEntryReader<V> getEntryReader()
    {
        return entryReader;
    }

    public void setEntryReader( JournalEntryReader<V> entryReader )
    {
        this.entryReader = entryReader;
    }

    public JournalEntryWriter<V> getEntryWriter()
    {
        return entryWriter;
    }

    public void setEntryWriter( JournalEntryWriter<V> entryWriter )
    {
        this.entryWriter = entryWriter;
    }

}
