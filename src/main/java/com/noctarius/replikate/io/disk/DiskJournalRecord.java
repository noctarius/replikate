package com.noctarius.replikate.io.disk;

import com.noctarius.replikate.JournalEntry;
import com.noctarius.replikate.JournalRecord;

class DiskJournalRecord<V>
    implements JournalRecord<V>
{

    private final JournalEntry<V> entry;

    private final long recordId;

    DiskJournalRecord( JournalEntry<V> entry, long recordId )
    {
        this.entry = entry;
        this.recordId = recordId;
    }

    @Override
    public int compareTo( JournalRecord<V> o )
    {
        return Long.valueOf( recordId ).compareTo( o.getRecordId() );
    }

    @Override
    public byte getType()
    {
        return entry.getType();
    }

    @Override
    public long getRecordId()
    {
        return recordId;
    }

    @Override
    public JournalEntry<V> getJournalEntry()
    {
        return entry;
    }

    @Override
    public String toString()
    {
        return "DiskJournalRecord [recordId=" + recordId + ", entry=" + entry + "]";
    }

}
