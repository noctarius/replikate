package com.github.noctarius.replikate.io.disk;

import java.io.File;
import java.util.Random;

import org.junit.Test;

import com.github.noctarius.replikate.Journal;
import com.github.noctarius.replikate.JournalBatch;
import com.github.noctarius.replikate.JournalEntry;
import com.github.noctarius.replikate.SimpleJournalEntry;
import com.github.noctarius.replikate.exceptions.JournalException;
import com.github.noctarius.replikate.io.disk.BasicDiskJournalTestCase.NamingStrategy;
import com.github.noctarius.replikate.io.disk.OverflowTestCase.FlushListener;
import com.github.noctarius.replikate.io.disk.OverflowTestCase.RecordIdGenerator;
import com.github.noctarius.replikate.io.disk.OverflowTestCase.RecordReader;
import com.github.noctarius.replikate.io.disk.OverflowTestCase.RecordWriter;

public class BatchProcessTestCase
    extends AbstractJournalTestCase
{

    @Test
    public void testSimpleBatchProcessSuccessful()
        throws Exception
    {
        File path = prepareJournalDirectory( "testSimpleBatchProcessSuccessful" );

        RecordIdGenerator recordIdGenerator = new RecordIdGenerator();
        Journal<byte[]> journal =
            new DiskJournal<>( "testSimpleBatchProcessSuccessful", path.toPath(), new FlushListener(), 1024,
                               recordIdGenerator, new RecordReader(), new RecordWriter(), new NamingStrategy() );

        JournalEntry<byte[]> record1 = buildTestRecord( (byte) 1 );
        JournalEntry<byte[]> record2 = buildTestRecord( (byte) 2 );
        JournalEntry<byte[]> record3 = buildTestRecord( (byte) 3 );

        JournalBatch<byte[]> batch = journal.startBatchProcess();
        batch.appendEntry( record1 );
        batch.appendEntry( record2 );
        batch.appendEntry( record3 );
        batch.commit();

        journal.close();
    }

    @Test( expected = JournalException.class )
    public void testSimpleBatchProcessAfterClose()
        throws Exception
    {
        File path = prepareJournalDirectory( "testSimpleBatchProcessAfterClose" );

        RecordIdGenerator recordIdGenerator = new RecordIdGenerator();
        Journal<byte[]> journal =
            new DiskJournal<>( "testSimpleBatchProcessAfterClose", path.toPath(), new FlushListener(), 1024,
                               recordIdGenerator, new RecordReader(), new RecordWriter(), new NamingStrategy() );

        JournalEntry<byte[]> record1 = buildTestRecord( (byte) 1 );
        JournalEntry<byte[]> record2 = buildTestRecord( (byte) 2 );
        JournalEntry<byte[]> record3 = buildTestRecord( (byte) 3 );

        JournalBatch<byte[]> batch = journal.startBatchProcess();
        batch.appendEntry( record1 );
        batch.appendEntry( record2 );
        batch.appendEntry( record3 );

        journal.close();

        batch.commit();
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

}