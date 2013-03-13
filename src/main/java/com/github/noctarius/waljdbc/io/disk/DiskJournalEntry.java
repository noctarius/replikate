package com.github.noctarius.waljdbc.io.disk;

import com.github.noctarius.waljdbc.SimpleJournalEntry;

class DiskJournalEntry<V>
    extends SimpleJournalEntry<V>
{

    byte[] cachedData = null;

    public DiskJournalEntry( V value, byte type )
    {
        super( value, type );
    }

}
