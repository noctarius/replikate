package com.github.noctarius.waljdbc;

public interface JournalRecord<V>
    extends Comparable<JournalRecord<V>>
{

    byte getType();

    long getRecordId();

    JournalEntry<V> getJournalEntry();

    void notifyRecordCommitted();

}
