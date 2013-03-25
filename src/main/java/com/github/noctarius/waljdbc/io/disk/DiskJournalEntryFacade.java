package com.github.noctarius.waljdbc.io.disk;

import com.github.noctarius.waljdbc.JournalEntry;

class DiskJournalEntryFacade<V>
    extends DiskJournalEntry<V>
{

    final JournalEntry<V> wrappedEntry;

    public DiskJournalEntryFacade( JournalEntry<V> wrappedEntry )
    {
        super( wrappedEntry.getValue(), wrappedEntry.getType() );
        this.wrappedEntry = wrappedEntry;
    }

}
