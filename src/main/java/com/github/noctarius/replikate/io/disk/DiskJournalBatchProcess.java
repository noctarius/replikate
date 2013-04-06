package com.github.noctarius.replikate.io.disk;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.github.noctarius.replikate.JournalBatch;
import com.github.noctarius.replikate.JournalEntry;
import com.github.noctarius.replikate.JournalListener;
import com.github.noctarius.replikate.exceptions.JournalException;

class DiskJournalBatchProcess<V>
    implements JournalBatch<V>
{

    private final AtomicBoolean committed = new AtomicBoolean( false );

    private final List<DiskJournalEntryFacade<V>> entries = new LinkedList<>();

    private final JournalListener<V> listener;

    private final DiskJournal<V> journal;

    private volatile int dataSize = 0;

    public DiskJournalBatchProcess( DiskJournal<V> journal, JournalListener<V> listener )
    {
        this.journal = journal;
        this.listener = listener;
    }

    @Override
    public void appendEntry( JournalEntry<V> entry )
        throws JournalException
    {
        try
        {
            DiskJournalEntryFacade<V> batchEntry = DiskJournalIOUtils.prepareJournalEntry( entry, journal.getWriter() );
            entries.add( batchEntry );
            dataSize += batchEntry.cachedData.length;
        }
        catch ( IOException e )
        {
            throw new JournalException( "JournalEntry could not be added to the batch job", e );
        }
    }

    @Override
    public void commit()
        throws JournalException
    {
        if ( !committed.compareAndSet( false, true ) )
        {
            throw new JournalException( "Batch already committed" );
        }

        journal.commitBatchProcess( this, entries, dataSize, listener );
    }

    @Override
    public void commitSynchronous()
        throws JournalException
    {
        if ( !committed.compareAndSet( false, true ) )
        {
            throw new JournalException( "Batch already committed" );
        }

        journal.commitBatchProcessSync( this, entries, dataSize, listener );
    }

    @Override
    public String toString()
    {
        return "DiskJournalBatchProcess [committed=" + committed + ", entries=" + entries + "]";
    }

}
