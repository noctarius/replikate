package com.noctarius.replikate.io.disk;

import com.noctarius.replikate.SimpleJournalEntry;

class DiskJournalEntry<V>
    extends SimpleJournalEntry<V>
{

    byte[] cachedData = null;

    public DiskJournalEntry( V value, byte type )
    {
        super( value, type );
    }

}
