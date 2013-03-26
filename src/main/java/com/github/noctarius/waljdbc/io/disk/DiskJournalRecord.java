package com.github.noctarius.waljdbc.io.disk;

import com.github.noctarius.waljdbc.JournalEntry;
import com.github.noctarius.waljdbc.JournalRecord;

class DiskJournalRecord<V>
    implements JournalRecord<V>
{

    private final JournalEntry<V> entry;

    private final long recordId;

    private final DiskJournal<V> journal;

    private final DiskJournalFile<V> journalFile;

    DiskJournalRecord( JournalEntry<V> entry, long recordId, DiskJournal<V> journal, DiskJournalFile<V> journalFile )
    {
        this.entry = entry;
        this.recordId = recordId;
        this.journal = journal;
        this.journalFile = journalFile;
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
    public void notifyCommitted()
    {
        journal.journalRecordCommitted( this, journalFile );
    }

    @Override
    public String toString()
    {
        return "DiskJournalRecord [recordId=" + recordId + ", entry=" + entry + "]";
    }

}
