package com.noctarius.replikate;

public interface JournalRecord<V>
    extends Comparable<JournalRecord<V>>
{

    byte getType();

    long getRecordId();

    JournalEntry<V> getJournalEntry();

}
