package com.github.noctarius.replikate.io.disk;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.github.noctarius.replikate.JournalBatch;
import com.github.noctarius.replikate.JournalEntry;
import com.github.noctarius.replikate.JournalRecord;
import com.github.noctarius.replikate.exceptions.JournalException;

public class DiskJournalBatchProcess<V>
    implements JournalBatch<V>
{

    private final AtomicBoolean committed = new AtomicBoolean( false );

    private final List<JournalRecord<V>> records = new LinkedList<>();

    @Override
    public void appendEntry( JournalEntry<V> entry )
        throws JournalException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void commit()
        throws JournalException
    {
        if ( committed.compareAndSet( false, true ) )
        {
            throw new JournalException( "Batch already committed" );
        }

        // TODO Auto-generated method stub

    }

}
