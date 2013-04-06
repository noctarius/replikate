package com.github.noctarius.replikate;

import com.github.noctarius.replikate.exceptions.JournalException;

public interface JournalBatch<V>
{

    void appendEntry( JournalEntry<V> entry )
        throws JournalException;

    void commit();

    void commitSynchronous()
        throws JournalException;

}
