package com.github.noctarius.waljdbc.io.disk;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.github.noctarius.waljdbc.JournalEntry;
import com.github.noctarius.waljdbc.spi.JournalEntryReader;

class DiskJournalRecord<V>
    implements Comparable<DiskJournalRecord<V>>
{

    private final JournalEntry<V> entry;

    private final long recordId;

    DiskJournalRecord( JournalEntry<V> entry, long recordId )
    {
        this.entry = entry;
        this.recordId = recordId;
    }

    @Override
    public int compareTo( DiskJournalRecord<V> o )
    {
        return Long.valueOf( recordId ).compareTo( o.recordId );
    }

    public byte getType()
    {
        return entry.getType();
    }

    public long getRecordId()
    {
        return recordId;
    }

    public JournalEntry<V> getJournalEntry()
    {
        return entry;
    }

    static <V> void writeRecord( DiskJournalRecord<V> record, byte[] entryData, RandomAccessFile raf )
        throws IOException
    {
        int minSize = DiskJournal.JOURNAL_RECORD_HEADER_SIZE + 100;

        try ( ByteArrayOutputStream out = new ByteArrayOutputStream( minSize );
                        DataOutputStream stream = new DataOutputStream( out ) )
        {
            int recordLength = DiskJournal.JOURNAL_RECORD_HEADER_SIZE + entryData.length;

            stream.writeInt( recordLength );
            stream.writeLong( record.getRecordId() );
            stream.writeByte( record.getType() );
            stream.write( entryData );
            stream.writeInt( recordLength );
            raf.write( out.toByteArray() );
        }
    }

    static <V> DiskJournalRecord<V> readRecord( JournalEntryReader<V> reader, RandomAccessFile raf )
        throws IOException
    {
        long pos = raf.getFilePointer();

        try
        {
            int startingLength = raf.readInt();
            long recordId = raf.readLong();

            int entryLength = startingLength - DiskJournal.JOURNAL_RECORD_HEADER_SIZE;
            byte type = raf.readByte();

            byte[] entryData = new byte[entryLength];
            raf.readFully( entryData );

            return new DiskJournalRecord<>( reader.readJournalEntry( recordId, type, entryData ), recordId );
        }
        catch ( IOException e )
        {
            // Will never throw IOException itself since pos > 0 < maxLength
            raf.seek( pos );
            throw e;
        }
    }

}
