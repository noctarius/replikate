package com.github.noctarius.replikate.io.disk;

import java.io.File;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import com.github.noctarius.replikate.Journal;
import com.github.noctarius.replikate.JournalBatch;
import com.github.noctarius.replikate.JournalConfiguration;
import com.github.noctarius.replikate.JournalEntry;
import com.github.noctarius.replikate.JournalRecord;
import com.github.noctarius.replikate.JournalSystem;
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
        JournalSystem journalSystem = JournalSystem.buildJournalSystem();
        JournalConfiguration<byte[]> configuration =
            buildDiskJournalConfiguration( path.toPath(), 1024, new RecordReader(), new RecordWriter(),
                                           new FlushListener(), new NamingStrategy(), recordIdGenerator );
        Journal<byte[]> journal = journalSystem.getJournal( "testSimpleBatchProcessSuccessful", configuration );

        JournalEntry<byte[]> record1 = buildTestRecord( (byte) 1 );
        JournalEntry<byte[]> record2 = buildTestRecord( (byte) 2 );
        JournalEntry<byte[]> record3 = buildTestRecord( (byte) 3 );

        JournalBatch<byte[]> batch = journal.startBatchProcess();
        batch.appendEntry( record1 );
        batch.appendEntry( record2 );
        batch.appendEntry( record3 );
        batch.commitSynchronous();

        journal.close();
    }

    @Test
    public void testSimpleBatchProcessAsyncSuccessful()
        throws Exception
    {
        File path = prepareJournalDirectory( "testSimpleBatchProcessAsyncSuccessful" );

        final CountDownLatch latch = new CountDownLatch( 3 );
        FlushListener flushListener = new FlushListener()
        {

            @Override
            public void onCommit( JournalRecord<byte[]> record )
            {
                latch.countDown();
                super.onCommit( record );
            }

            @Override
            public void onFailure( JournalEntry<byte[]> entry, JournalException cause )
            {
                latch.countDown();
                latch.countDown();
                latch.countDown();
                super.onFailure( entry, cause );
            }

            @Override
            public void onFailure( JournalBatch<byte[]> journalBatch, JournalException cause )
            {
                latch.countDown();
                super.onFailure( journalBatch, cause );
            }
        };

        RecordIdGenerator recordIdGenerator = new RecordIdGenerator();
        JournalSystem journalSystem = JournalSystem.buildJournalSystem();
        JournalConfiguration<byte[]> configuration =
            buildDiskJournalConfiguration( path.toPath(), 1024, new RecordReader(), new RecordWriter(), flushListener,
                                           new NamingStrategy(), recordIdGenerator );

        Journal<byte[]> journal = journalSystem.getJournal( "testSimpleBatchProcessAsyncSuccessful", configuration );

        JournalEntry<byte[]> record1 = buildTestRecord( (byte) 1 );
        JournalEntry<byte[]> record2 = buildTestRecord( (byte) 2 );
        JournalEntry<byte[]> record3 = buildTestRecord( (byte) 3 );

        JournalBatch<byte[]> batch = journal.startBatchProcess();
        batch.appendEntry( record1 );
        batch.appendEntry( record2 );
        batch.appendEntry( record3 );

        batch.commit();
        latch.await();
        journal.close();
    }

    @Test( expected = JournalException.class )
    public void testSimpleBatchProcessAfterClose()
        throws Exception
    {
        File path = prepareJournalDirectory( "testSimpleBatchProcessAfterClose" );

        RecordIdGenerator recordIdGenerator = new RecordIdGenerator();
        JournalSystem journalSystem = JournalSystem.buildJournalSystem();
        JournalConfiguration<byte[]> configuration =
            buildDiskJournalConfiguration( path.toPath(), 1024, new RecordReader(), new RecordWriter(),
                                           new FlushListener(), new NamingStrategy(), recordIdGenerator );

        Journal<byte[]> journal = journalSystem.getJournal( "testSimpleBatchProcessAfterClose", configuration );

        JournalEntry<byte[]> record1 = buildTestRecord( (byte) 1 );
        JournalEntry<byte[]> record2 = buildTestRecord( (byte) 2 );
        JournalEntry<byte[]> record3 = buildTestRecord( (byte) 3 );

        JournalBatch<byte[]> batch = journal.startBatchProcess();
        batch.appendEntry( record1 );
        batch.appendEntry( record2 );
        batch.appendEntry( record3 );

        journal.close();

        batch.commitSynchronous();
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
