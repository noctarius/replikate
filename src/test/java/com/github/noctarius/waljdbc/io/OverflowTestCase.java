package com.github.noctarius.waljdbc.io;

import static org.junit.Assert.assertEquals;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.github.noctarius.waljdbc.Journal;
import com.github.noctarius.waljdbc.JournalEntry;
import com.github.noctarius.waljdbc.SimpleJournalEntry;
import com.github.noctarius.waljdbc.exceptions.JournalException;
import com.github.noctarius.waljdbc.io.BasicDiskJournalTestCase.NamingStrategy;
import com.github.noctarius.waljdbc.io.disk.DiskJournal;
import com.github.noctarius.waljdbc.spi.JournalEntryReader;
import com.github.noctarius.waljdbc.spi.JournalEntryWriter;
import com.github.noctarius.waljdbc.spi.JournalFlushedListener;
import com.github.noctarius.waljdbc.spi.JournalRecordIdGenerator;
import com.github.noctarius.waljdbc.spi.ReplayNotificationResult;

public class OverflowTestCase
{

    @Test
    public void testSimpleFileOverflow()
        throws Exception
    {
        File path = new File( "target/journals/testSimpleFileOverflow" );
        path.mkdirs();

        RecordIdGenerator recordIdGenerator = new RecordIdGenerator();
        DiskJournal<byte[]> journal =
            new DiskJournal<>( "testSimpleFileOverflow", path.toPath(), new FlushListener(), 1024, recordIdGenerator,
                               new RecordReader(), new RecordWriter(), new NamingStrategy() );

        JournalEntry<byte[]> record1 = buildTestRecord( (byte) 1 );
        JournalEntry<byte[]> record2 = buildTestRecord( (byte) 2 );
        JournalEntry<byte[]> record3 = buildTestRecord( (byte) 3 );

        journal.appendEntry( record1 );
        journal.appendEntry( record2 );

        // Here the journal should overflow
        journal.appendEntry( record3 );

        journal.close();

        File file = new File( path, "journal-1" );
        RandomAccessFile raf = new RandomAccessFile( file, "rws" );

        raf.seek( file.length() - 1 );
        while ( raf.readByte() == 0 )
        {
            raf.seek( raf.getFilePointer() - 2 );
        }
        long length = raf.getFilePointer() - 20;
        raf.setLength( length );
        raf.close();

        assertEquals( length, file.length() );

        CountingFlushListener listener = new CountingFlushListener( ReplayNotificationResult.Except );
        journal =
            new DiskJournal<>( "testSimpleFileOverflow", path.toPath(), listener, 1024 * 1024, new RecordIdGenerator(),
                               new RecordReader(), new RecordWriter(), new NamingStrategy() );

        assertEquals( 3, listener.count );

        JournalEntry<byte[]> result1 = listener.get( 0 );
        JournalEntry<byte[]> result2 = listener.get( 1 );
        JournalEntry<byte[]> result3 = listener.get( 2 );

        assertEquals( record1, result1 );
        assertEquals( record2, result2 );
        assertEquals( record3, result3 );
    }

    private SimpleJournalEntry<byte[]> buildTestRecord( byte type )
    {
        Random random = new Random( -System.nanoTime() );
        byte[] data = new byte[400];
        for ( int i = 0; i < data.length; i++ )
        {
            data[i] = (byte) random.nextInt( 255 );
        }
        return new SimpleJournalEntry<byte[]>( data, type );
    }

    public static class RecordReader
        implements JournalEntryReader<byte[]>
    {

        @Override
        public JournalEntry<byte[]> readJournalEntry( long recordId, byte type, byte[] data )
            throws IOException
        {
            return new SimpleJournalEntry<byte[]>( data, type );
        }

    }

    public static class RecordWriter
        implements JournalEntryWriter<byte[]>
    {

        @Override
        public void writeJournalEntry( JournalEntry<byte[]> entry, DataOutput out )
            throws IOException
        {
            out.write( entry.getValue() );
        }

    }

    public static class FlushListener
        implements JournalFlushedListener<byte[]>
    {

        @Override
        public void flushed( JournalEntry<byte[]> entry )
        {
            // TODO Auto-generated method stub

        }

        @Override
        public void failed( JournalEntry<byte[]> entry, JournalException cause )
        {
            // TODO Auto-generated method stub

        }

        @Override
        public ReplayNotificationResult replayNotifySuspiciousRecordId( Journal<byte[]> journal,
                                                                        JournalEntry<byte[]> lastEntry,
                                                                        JournalEntry<byte[]> currentEntry )
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ReplayNotificationResult replayRecordId( Journal<byte[]> journal, JournalEntry<byte[]> entry )
        {
            // TODO Auto-generated method stub
            return null;
        }

    }

    public static class RecordIdGenerator
        implements JournalRecordIdGenerator
    {

        private long recordId = 0;

        @Override
        public synchronized long nextRecordId()
        {
            return ++recordId;
        }

        @Override
        public long lastGeneratedRecordId()
        {
            return recordId;
        }

    }

    public static class CountingFlushListener
        extends FlushListener
    {

        private int count;

        private final List<JournalEntry<byte[]>> records = new ArrayList<>();

        private final ReplayNotificationResult missingRecordIdResult;

        public CountingFlushListener()
        {
            this( ReplayNotificationResult.Continue );
        }

        public CountingFlushListener( ReplayNotificationResult missingRecordIdResult )
        {
            this.missingRecordIdResult = missingRecordIdResult;
        }

        @Override
        public ReplayNotificationResult replayNotifySuspiciousRecordId( Journal<byte[]> journal,
                                                                        JournalEntry<byte[]> lastEntry,
                                                                        JournalEntry<byte[]> nextEntry )
        {
            return missingRecordIdResult;
        }

        @Override
        public ReplayNotificationResult replayRecordId( Journal<byte[]> journal, JournalEntry<byte[]> entry )
        {
            records.add( entry );
            count++;
            return super.replayRecordId( journal, entry );
        }

        public int getCount()
        {
            return count;
        }

        public JournalEntry<byte[]> get( int index )
        {
            return records.get( index );
        }

    }

}
