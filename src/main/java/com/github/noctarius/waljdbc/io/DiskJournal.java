package com.github.noctarius.waljdbc.io;

import com.github.noctarius.waljdbc.AbstractJournal;
import com.github.noctarius.waljdbc.JournalEntry;
import com.github.noctarius.waljdbc.JournalFlushedListener;

public class DiskJournal<V>
    extends AbstractJournal<V>
{

    private static final int JOURNAL_FILE_HEADER_SIZE = 17;

    private static final int JOURNAL_RECORD_HEADER_SIZE = 9;

    public DiskJournal( int maxLogFileSize )
    {
        super( maxLogFileSize );
    }

    @Override
    public void appendEntry( JournalEntry<V> entry, JournalFlushedListener<V> listener )
    {
        
        
        
    }

}
