package com.github.noctarius.waljdbc.io.disk;

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
import com.github.noctarius.waljdbc.JournalRecord;
import com.github.noctarius.waljdbc.SimpleJournalEntry;
import com.github.noctarius.waljdbc.exceptions.JournalException;
import com.github.noctarius.waljdbc.io.disk.DiskJournal;
import com.github.noctarius.waljdbc.io.disk.BasicDiskJournalTestCase.NamingStrategy;
import com.github.noctarius.waljdbc.spi.JournalEntryReader;
import com.github.noctarius.waljdbc.spi.JournalEntryWriter;
import com.github.noctarius.waljdbc.spi.JournalListener;
import com.github.noctarius.waljdbc.spi.JournalRecordIdGenerator;
import com.github.noctarius.waljdbc.spi.ReplayNotificationResult;

public class OverflowTestCase
    extends AbstractJournalTestCase
{

    @Test
    public void testSimpleFileOverflow()
        throws Exception
    {
        File path = prepareJournalDirectory( "testSimpleFileOverflow" );

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

    @Test
    public void testMultipleSimpleFileOverflow()
        throws Exception
    {
        File path = prepareJournalDirectory( "testMultipleSimpleFileOverflow" );

        RecordIdGenerator recordIdGenerator = new RecordIdGenerator();
        DiskJournal<byte[]> journal =
            new DiskJournal<>( "testMultipleSimpleFileOverflow", path.toPath(), new FlushListener(), 4096,
                               recordIdGenerator, new RecordReader(), new RecordWriter(), new NamingStrategy() );

        @SuppressWarnings( "unchecked" )
        JournalEntry<byte[]>[] records = new JournalEntry[50];
        for ( int i = 0; i < records.length; i++ )
        {
            records[i] = buildTestRecord( (byte) i );
            journal.appendEntry( records[i] );
        }

        journal.close();

        CountingFlushListener listener = new CountingFlushListener( ReplayNotificationResult.Except );
        journal =
            new DiskJournal<>( "testMultipleSimpleFileOverflow", path.toPath(), listener, 1024 * 1024,
                               new RecordIdGenerator(), new RecordReader(), new RecordWriter(), new NamingStrategy() );

        assertEquals( records.length, listener.count );

        for ( int i = 0; i < records.length; i++ )
        {
            JournalEntry<byte[]> result = listener.get( i );
            assertEquals( records[i], result );
        }
    }

    @Test
    public void testFullFileOverflow()
        throws Exception
    {
        File path = prepareJournalDirectory( "testFullFileOverflow" );

        RecordIdGenerator recordIdGenerator = new RecordIdGenerator();
        DiskJournal<byte[]> journal =
            new DiskJournal<>( "testFullFileOverflow", path.toPath(), new FlushListener(), 1024, recordIdGenerator,
                               new RecordReader(), new RecordWriter(), new NamingStrategy() );

        @SuppressWarnings( "unchecked" )
        JournalEntry<byte[]>[] records = new JournalEntry[5];
        for ( int i = 0; i < records.length; i++ )
        {
            // Generate special overflow entry on index == 2
            records[i] = buildTestRecord( i == 2 ? 1024 : 400, (byte) i );
        }

        journal.appendEntry( records[0] );
        journal.appendEntry( records[1] );
        journal.appendEntry( records[2] );
        journal.appendEntry( records[3] );
        journal.appendEntry( records[4] );

        journal.close();

        // 3 journal files needs to be generated
        File[] files = path.listFiles();
        assertEquals( 3, files.length );

        // "journal-2" needs to be overflow file
        RandomAccessFile raf = new RandomAccessFile( new File( path, "journal-2" ), "r" );
        DiskJournalFileHeader header = DiskJournalIOUtils.readHeader( raf );
        assertEquals( DiskJournal.JOURNAL_FILE_TYPE_OVERFLOW, header.getType() );
        raf.close();

        CountingFlushListener listener = new CountingFlushListener( ReplayNotificationResult.Except );
        journal =
            new DiskJournal<>( "testFullFileOverflow", path.toPath(), listener, 1024 * 1024, new RecordIdGenerator(),
                               new RecordReader(), new RecordWriter(), new NamingStrategy() );

        assertEquals( records.length, listener.count );

        for ( int i = 0; i < records.length; i++ )
        {
            JournalEntry<byte[]> result = listener.get( i );
            assertEquals( records[i], result );
        }
    }

    private SimpleJournalEntry<byte[]> buildTestRecord( byte type )
    {
        return buildTestRecord( 400, type );
    }

    private SimpleJournalEntry<byte[]> buildTestRecord( int dataLength, byte type )
    {
        Random random = new Random( -System.nanoTime() );
        byte[] data = new byte[dataLength];
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
        implements JournalListener<byte[]>
    {

        @Override
        public void flushed( JournalRecord<byte[]> record )
        {
            System.out.println( "flushed: " + record );
        }

        @Override
        public void failed( JournalEntry<byte[]> entry, JournalException cause )
        {
            System.out.println( "failed: " + entry );
        }

        @Override
        public ReplayNotificationResult replayNotifySuspiciousRecordId( Journal<byte[]> journal,
                                                                        JournalRecord<byte[]> lastRecord,
                                                                        JournalRecord<byte[]> nextRecord )
        {
            return ReplayNotificationResult.Continue;
        }

        @Override
        public ReplayNotificationResult replayRecordId( Journal<byte[]> journal, JournalRecord<byte[]> record )
        {
            return ReplayNotificationResult.Continue;
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

        @Override
        public void notifyHighestJournalRecordId( long recordId )
        {
            this.recordId = recordId;
        }

    }

    public static class CountingFlushListener
        extends FlushListener
    {

        private int count;

        private final List<JournalRecord<byte[]>> records = new ArrayList<>();

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
                                                                        JournalRecord<byte[]> lastRecord,
                                                                        JournalRecord<byte[]> nextRecord )
        {
            return missingRecordIdResult;
        }

        @Override
        public ReplayNotificationResult replayRecordId( Journal<byte[]> journal, JournalRecord<byte[]> record )
        {
            records.add( record );
            count++;
            return super.replayRecordId( journal, record );
        }

        public int getCount()
        {
            return count;
        }

        public JournalEntry<byte[]> get( int index )
        {
            return records.get( index ).getJournalEntry();
        }

    }

}
