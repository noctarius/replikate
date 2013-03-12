package com.github.noctarius.waljdbc.io;

import java.io.IOException;
import java.io.RandomAccessFile;

import com.github.noctarius.waljdbc.JournalEntry;
import com.github.noctarius.waljdbc.spi.JournalEntryReader;
import com.github.noctarius.waljdbc.spi.JournalEntryWriter;

class DiskJournalRecord<V>
{

    private final JournalEntry<V> entry;

    private final long recordId;

    private final int length;

    DiskJournalRecord( JournalEntry<V> entry, long recordId )
    {
        this.entry = entry;
        this.recordId = recordId;
        this.length = DiskJournal.JOURNAL_RECORD_HEADER_SIZE + entry.getLength();
    }

    public byte[] getData()
    {
        return entry.getData();
    }

    public byte getType()
    {
        return entry.getType();
    }

    public long getRecordId()
    {
        return recordId;
    }

    public int getLength()
    {
        return length;
    }

    public static <V> void writeRecord( DiskJournalRecord<V> record, JournalEntryWriter<V> writer, RandomAccessFile raf )
        throws IOException
    {
        try ( DataByteArrayOutputBuffer stream = new DataByteArrayOutputBuffer( record.getLength() ) )
        {
            stream.writeInt( record.getLength() );
            stream.writeLong( record.getRecordId() );
            writer.writeJournalEntry( record.entry, stream );
            stream.writeInt( record.getLength() );
            raf.write( stream.toByteArray() );
        }
    }

    public static <V> DiskJournalRecord<V> readRecord( JournalEntryReader<V> reader, RandomAccessFile raf )
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
