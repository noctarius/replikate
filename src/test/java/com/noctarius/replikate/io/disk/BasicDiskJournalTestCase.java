/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package com.noctarius.replikate.io.disk;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.noctarius.replikate.Journal;
import com.noctarius.replikate.JournalBatch;
import com.noctarius.replikate.JournalConfiguration;
import com.noctarius.replikate.JournalEntry;
import com.noctarius.replikate.JournalListener;
import com.noctarius.replikate.JournalNamingStrategy;
import com.noctarius.replikate.JournalRecord;
import com.noctarius.replikate.JournalSystem;
import com.noctarius.replikate.SimpleJournalEntry;
import com.noctarius.replikate.exceptions.JournalException;
import com.noctarius.replikate.exceptions.ReplayCancellationException;
import com.noctarius.replikate.spi.JournalEntryReader;
import com.noctarius.replikate.spi.JournalEntryWriter;
import com.noctarius.replikate.spi.JournalRecordIdGenerator;
import com.noctarius.replikate.spi.ReplayNotificationResult;

public class BasicDiskJournalTestCase
    extends AbstractJournalTestCase
{

    private static final String TESTCHARACTERS =
        "QWERTZUIOPÜASDFGHJKLÖÄYXCVBNM;:_*'Ä_?=)(/&%$§!qwertzuiopü+#äölkjhgfdsayxcvbnm,.-@@ł€¶ŧ←↓→øþſðđŋħł«¢„“”µ·…";

    @Test
    public void createJournalFile()
        throws Exception
    {
        File path = prepareJournalDirectory( "createJournalFile" );

        JournalSystem journalSystem = JournalSystem.buildJournalSystem();
        JournalConfiguration<TestRecord> configuration =
            buildDiskJournalConfiguration( path.toPath(), 1024 * 1024, new TestRecordReader(), new TestRecordWriter(),
                                           new FlushListener(), new NamingStrategy(), new RecordIdGenerator() );

        Journal<TestRecord> journal = journalSystem.getJournal( "createJournalFile", configuration );

        journal.close();

        CountingFlushListener listener = new CountingFlushListener();
        configuration.setListener( listener );
        journal = journalSystem.getJournal( "createJournalFile", configuration );

        journal.close();

        assertEquals( 0, listener.getCount() );
    }

    @Test
    public void appendEntries()
        throws Exception
    {
        File path = prepareJournalDirectory( "appendEntries" );

        JournalSystem journalSystem = JournalSystem.buildJournalSystem();
        JournalConfiguration<TestRecord> configuration =
            buildDiskJournalConfiguration( path.toPath(), 1024 * 1024, new TestRecordReader(), new TestRecordWriter(),
                                           new FlushListener(), new NamingStrategy(), new RecordIdGenerator() );

        Journal<TestRecord> journal = journalSystem.getJournal( "appendEntries", configuration );

        JournalEntry<TestRecord> record1 = buildTestRecord( 1, "test1", (byte) 12 );
        JournalEntry<TestRecord> record2 = buildTestRecord( 2, "test2", (byte) 24 );
        JournalEntry<TestRecord> record3 = buildTestRecord( 4, "test3", (byte) 32 );
        JournalEntry<TestRecord> record4 = buildTestRecord( 8, "test4", (byte) 48 );

        journal.appendEntrySynchronous( record1 );
        journal.appendEntrySynchronous( record2 );
        journal.appendEntrySynchronous( record3 );
        journal.appendEntrySynchronous( record4 );

        journal.close();

        CountingFlushListener listener = new CountingFlushListener();
        configuration.setListener( listener );
        journal = journalSystem.getJournal( "appendEntries", configuration );

        journal.close();

        assertEquals( 4, listener.getCount() );
        assertEquals( record1, listener.get( 0 ) );
        assertEquals( record2, listener.get( 1 ) );
        assertEquals( record3, listener.get( 2 ) );
        assertEquals( record4, listener.get( 3 ) );
    }

    @Test
    public void loadBrokenJournal()
        throws Exception
    {
        File path = prepareJournalDirectory( "loadBrokenJournal" );

        JournalSystem journalSystem = JournalSystem.buildJournalSystem();
        JournalConfiguration<TestRecord> configuration =
            buildDiskJournalConfiguration( path.toPath(), 1024 * 1024, new TestRecordReader(), new TestRecordWriter(),
                                           new FlushListener(), new NamingStrategy(), new RecordIdGenerator() );

        Journal<TestRecord> journal = journalSystem.getJournal( "loadBrokenJournal", configuration );

        JournalEntry<TestRecord> record1 = buildTestRecord( 1, "test1", (byte) 12 );
        JournalEntry<TestRecord> record2 = buildTestRecord( 2, "test2", (byte) 24 );
        JournalEntry<TestRecord> record3 = buildTestRecord( 4, "test3", (byte) 32 );
        JournalEntry<TestRecord> record4 = buildTestRecord( 8, "test4", (byte) 48 );

        journal.appendEntrySynchronous( record1 );
        journal.appendEntrySynchronous( record2 );
        journal.appendEntrySynchronous( record3 );
        journal.appendEntrySynchronous( record4 );

        journal.close();

        File file = new File( path, "journal-1" );
        RandomAccessFile raf = new RandomAccessFile( file, "rws" );

        byte[] data = new byte[(int) raf.length()];
        raf.read( data );

        int pos = data.length - 1;
        while ( data[pos] == 0 )
        {
            pos--;
        }
        raf.seek( pos - 6 );

        data = new byte[7];
        raf.write( data );
        raf.close();

        CountingFlushListener listener = new CountingFlushListener();
        configuration.setListener( listener );
        journal = journalSystem.getJournal( "loadBrokenJournal", configuration );

        journal.close();

        assertEquals( 3, listener.getCount() );
        assertEquals( record1, listener.get( 0 ) );
        assertEquals( record2, listener.get( 1 ) );
        assertEquals( record3, listener.get( 2 ) );
    }

    @Test
    public void loadShortenedJournal()
        throws Exception
    {
        File path = prepareJournalDirectory( "loadShortenedJournal" );

        JournalSystem journalSystem = JournalSystem.buildJournalSystem();
        JournalConfiguration<TestRecord> configuration =
            buildDiskJournalConfiguration( path.toPath(), 1024 * 1024, new TestRecordReader(), new TestRecordWriter(),
                                           new FlushListener(), new NamingStrategy(), new RecordIdGenerator() );

        Journal<TestRecord> journal = journalSystem.getJournal( "loadShortenedJournal", configuration );

        JournalEntry<TestRecord> record1 = buildTestRecord( 1, "test1", (byte) 12 );
        JournalEntry<TestRecord> record2 = buildTestRecord( 2, "test2", (byte) 24 );
        JournalEntry<TestRecord> record3 = buildTestRecord( 4, "test3", (byte) 32 );
        JournalEntry<TestRecord> record4 = buildTestRecord( 8, "test4", (byte) 48 );

        journal.appendEntrySynchronous( record1 );
        journal.appendEntrySynchronous( record2 );
        journal.appendEntrySynchronous( record3 );
        journal.appendEntrySynchronous( record4 );

        journal.close();

        File file = new File( path, "journal-1" );
        RandomAccessFile raf = new RandomAccessFile( file, "rws" );

        byte[] data = new byte[(int) raf.length()];
        raf.read( data );

        int pos = data.length - 1;
        while ( data[pos] == 0 )
        {
            pos--;
        }

        long length = pos - 20;
        raf.setLength( length );
        raf.close();

        assertEquals( length, file.length() );

        CountingFlushListener listener = new CountingFlushListener();
        configuration.setListener( listener );
        journal = journalSystem.getJournal( "loadShortenedJournal", configuration );

        journal.close();

        assertEquals( 3, listener.getCount() );
        assertEquals( record1, listener.get( 0 ) );
        assertEquals( record2, listener.get( 1 ) );
        assertEquals( record3, listener.get( 2 ) );
    }

    @Test
    public void findHolesInJournalAndAcceptIt()
        throws Exception
    {
        File path = prepareJournalDirectory( "findHolesInJournalAndAcceptIt" );

        RecordIdGenerator recordIdGenerator = new RecordIdGenerator();

        JournalSystem journalSystem = JournalSystem.buildJournalSystem();
        JournalConfiguration<TestRecord> configuration =
            buildDiskJournalConfiguration( path.toPath(), 1024 * 1024, new TestRecordReader(), new TestRecordWriter(),
                                           new FlushListener(), new NamingStrategy(), recordIdGenerator );

        Journal<TestRecord> journal = journalSystem.getJournal( "findHolesInJournalAndAcceptIt", configuration );

        JournalEntry<TestRecord> record1 = buildTestRecord( 1, "test1", (byte) 12 );
        JournalEntry<TestRecord> record2 = buildTestRecord( 2, "test2", (byte) 24 );
        JournalEntry<TestRecord> record3 = buildTestRecord( 4, "test3", (byte) 32 );
        JournalEntry<TestRecord> record4 = buildTestRecord( 8, "test4", (byte) 48 );

        journal.appendEntrySynchronous( record1 );
        journal.appendEntrySynchronous( record2 );

        // Force to create a hole in the journal file (in normal operation that should not happen)
        recordIdGenerator.recordId++;

        journal.appendEntrySynchronous( record3 );
        journal.appendEntrySynchronous( record4 );

        journal.close();

        CountingFlushListener listener = new CountingFlushListener();
        configuration.setListener( listener );
        journal = journalSystem.getJournal( "findHolesInJournalAndAcceptIt", configuration );

        journal.close();

        assertEquals( 4, listener.getCount() );
        assertEquals( record1, listener.get( 0 ) );
        assertEquals( record2, listener.get( 1 ) );
        assertEquals( record3, listener.get( 2 ) );
        assertEquals( record4, listener.get( 3 ) );
    }

    @Test( expected = ReplayCancellationException.class )
    public void findHolesInJournalAndDeclineIt()
        throws Exception
    {
        File path = prepareJournalDirectory( "findHolesInJournalAndDeclineIt" );

        RecordIdGenerator recordIdGenerator = new RecordIdGenerator();

        JournalSystem journalSystem = JournalSystem.buildJournalSystem();
        JournalConfiguration<TestRecord> configuration =
            buildDiskJournalConfiguration( path.toPath(), 1024 * 1024, new TestRecordReader(), new TestRecordWriter(),
                                           new FlushListener(), new NamingStrategy(), recordIdGenerator );

        Journal<TestRecord> journal = journalSystem.getJournal( "findHolesInJournalAndDeclineIt", configuration );

        JournalEntry<TestRecord> record1 = buildTestRecord( 1, "test1", (byte) 12 );
        JournalEntry<TestRecord> record2 = buildTestRecord( 2, "test2", (byte) 24 );
        JournalEntry<TestRecord> record3 = buildTestRecord( 4, "test3", (byte) 32 );
        JournalEntry<TestRecord> record4 = buildTestRecord( 8, "test4", (byte) 48 );

        journal.appendEntrySynchronous( record1 );
        journal.appendEntrySynchronous( record2 );

        // Force to create a hole in the journal file (in normal operation that should not happen)
        recordIdGenerator.recordId++;

        journal.appendEntrySynchronous( record3 );
        journal.appendEntrySynchronous( record4 );

        journal.close();

        try
        {
            CountingFlushListener listener = new CountingFlushListener( ReplayNotificationResult.Except );
            configuration.setListener( listener );
            journal = journalSystem.getJournal( "findHolesInJournalAndDeclineIt", configuration );
        }
        catch ( JournalException e )
        {
            e.printStackTrace();
            throw e;
        }
    }

    private SimpleJournalEntry<TestRecord> buildTestRecord( int value, String name, byte type )
    {
        Random random = new Random( -System.nanoTime() );

        int stringLength = random.nextInt( 100 );
        StringBuilder sb = new StringBuilder( stringLength );
        for ( int i = 0; i < stringLength; i++ )
        {
            int charPos = random.nextInt( TESTCHARACTERS.length() );
            sb.append( TESTCHARACTERS.toCharArray()[charPos] );
        }

        TestRecord record = new TestRecord();
        record.value = value + random.nextInt( 1000000 );
        record.name = name + "-" + sb.toString();
        return new SimpleJournalEntry<TestRecord>( record, type );
    }

    public static class TestRecord
    {

        private int value;

        private String name;

        @Override
        public String toString()
        {
            return "TestRecord [value=" + value + ", name=" + name + "]";
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ( ( name == null ) ? 0 : name.hashCode() );
            result = prime * result + value;
            return result;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj )
                return true;
            if ( obj == null )
                return false;
            if ( getClass() != obj.getClass() )
                return false;
            TestRecord other = (TestRecord) obj;
            if ( name == null )
            {
                if ( other.name != null )
                    return false;
            }
            else if ( !name.equals( other.name ) )
                return false;
            if ( value != other.value )
                return false;
            return true;
        }
    }

    public static class TestRecordReader
        implements JournalEntryReader<TestRecord>
    {

        @Override
        public JournalEntry<TestRecord> readJournalEntry( long recordId, byte type, byte[] data )
            throws IOException
        {
            try ( ByteArrayInputStream in = new ByteArrayInputStream( data );
                            DataInputStream buffer = new DataInputStream( in ) )
            {
                int value = buffer.readInt();
                String name = buffer.readUTF();
                TestRecord record = new TestRecord();
                record.value = value;
                record.name = name;
                return new SimpleJournalEntry<BasicDiskJournalTestCase.TestRecord>( record, type );
            }
        }
    }

    public static class TestRecordWriter
        implements JournalEntryWriter<TestRecord>
    {

        @Override
        public void writeJournalEntry( JournalEntry<TestRecord> entry, DataOutput out )
            throws IOException
        {
            TestRecord record = entry.getValue();
            out.writeInt( record.value );
            out.writeUTF( record.name );
        }

        @Override
        public int estimateRecordSize( JournalEntry<TestRecord> entry )
        {
            return 0;
        }

        @Override
        public boolean isRecordSizeEstimatable()
        {
            return false;
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

    public static class NamingStrategy
        implements JournalNamingStrategy
    {

        @Override
        public String generate( long logNumber )
        {
            return "journal-" + String.valueOf( logNumber );
        }

        @Override
        public boolean isJournal( String name )
        {
            return name.startsWith( "journal-" );
        }

        @Override
        public long extractLogNumber( String name )
        {
            return Integer.parseInt( name.substring( name.indexOf( '-' ) + 1 ) );
        }
    }

    public static class FlushListener
        implements JournalListener<TestRecord>
    {

        @Override
        public void onCommit( JournalRecord<TestRecord> record )
        {
            System.out.println( "flushed: " + record );
        }

        @Override
        public void onFailure( JournalEntry<TestRecord> entry, JournalException cause )
        {
            System.out.println( "failed: " + entry );
        }

        @Override
        public void onFailure( JournalBatch<TestRecord> journalBatch, JournalException cause )
        {
            System.out.println( "failed: " + journalBatch );
        }

        @Override
        public ReplayNotificationResult onReplaySuspiciousRecordId( Journal<TestRecord> journal,
                                                                    JournalRecord<TestRecord> lastRecord,
                                                                    JournalRecord<TestRecord> nextRecord )
        {
            return ReplayNotificationResult.Continue;
        }

        @Override
        public ReplayNotificationResult onReplayRecordId( Journal<TestRecord> journal, JournalRecord<TestRecord> record )
        {
            return ReplayNotificationResult.Continue;
        }
    }

    public static class CountingFlushListener
        extends FlushListener
    {

        private int count;

        private final List<JournalRecord<TestRecord>> records = new ArrayList<>();

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
        public ReplayNotificationResult onReplaySuspiciousRecordId( Journal<TestRecord> journal,
                                                                    JournalRecord<TestRecord> lastRecord,
                                                                    JournalRecord<TestRecord> nextRecord )
        {
            return missingRecordIdResult;
        }

        @Override
        public ReplayNotificationResult onReplayRecordId( Journal<TestRecord> journal, JournalRecord<TestRecord> record )
        {
            records.add( record );
            count++;
            return super.onReplayRecordId( journal, record );
        }

        public int getCount()
        {
            return count;
        }

        public JournalEntry<TestRecord> get( int index )
        {
            return records.get( index ).getJournalEntry();
        }
    }

}
