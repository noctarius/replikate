package com.github.noctarius.waljdbc.io;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.junit.Test;

import com.github.noctarius.waljdbc.JournalEntry;
import com.github.noctarius.waljdbc.JournalException;
import com.github.noctarius.waljdbc.SimpleJournalEntry;
import com.github.noctarius.waljdbc.io.disk.DiskJournal;
import com.github.noctarius.waljdbc.spi.JournalEntryReader;
import com.github.noctarius.waljdbc.spi.JournalEntryWriter;
import com.github.noctarius.waljdbc.spi.JournalFlushedListener;
import com.github.noctarius.waljdbc.spi.JournalNamingStrategy;
import com.github.noctarius.waljdbc.spi.JournalRecordIdGenerator;

public class BasicDiskJournalTest
{

    @Test
    public void createJournalFile()
        throws Exception
    {
        File path = new File( "target/journals/createJournalFile" );
        path.mkdirs();

        DiskJournal<TestRecord> journal =
            new DiskJournal<>( "createJournalFile", path.toPath(), new FlushListener(), 1024 * 1024,
                               new RecordIdGenerator(), new TestRecordReader(), new TestRecordWriter(),
                               new NamingStrategy() );

    }

    @Test
    public void appendEntries()
        throws Exception
    {
        File path = new File( "target/journals/appendEntries" );
        path.mkdirs();

        DiskJournal<TestRecord> journal =
            new DiskJournal<>( "appendEntries", path.toPath(), new FlushListener(), 1024 * 1024,
                               new RecordIdGenerator(), new TestRecordReader(), new TestRecordWriter(),
                               new NamingStrategy() );

        journal.appendEntry( buildTestRecord( 1, "test1", (byte) 12 ) );
        journal.appendEntry( buildTestRecord( 2, "test2", (byte) 24 ) );
        journal.appendEntry( buildTestRecord( 4, "test3", (byte) 32 ) );
        journal.appendEntry( buildTestRecord( 8, "test4", (byte) 48 ) );
    }

    @Test
    public void loadBrokenJournal()
        throws Exception
    {
        File path = new File( "target/journals/loadBrokenJournal" );
        path.mkdirs();

        DiskJournal<TestRecord> journal =
            new DiskJournal<>( "loadBrokenJournal", path.toPath(), new FlushListener(), 1024 * 1024,
                               new RecordIdGenerator(), new TestRecordReader(), new TestRecordWriter(),
                               new NamingStrategy() );

        journal.appendEntry( buildTestRecord( 1, "test1", (byte) 12 ) );
        journal.appendEntry( buildTestRecord( 2, "test2", (byte) 24 ) );
        journal.appendEntry( buildTestRecord( 4, "test3", (byte) 32 ) );
        journal.appendEntry( buildTestRecord( 8, "test4", (byte) 48 ) );

        journal.close();

        RandomAccessFile raf = new RandomAccessFile( new File( path, "journal-1" ), "rws" );
        raf.seek( 130 );
        byte[] data = new byte[7];
        raf.write( data );
        raf.close();

        journal =
            new DiskJournal<>( "loadBrokenJournal", path.toPath(), new FlushListener(), 1024 * 1024,
                               new RecordIdGenerator(), new TestRecordReader(), new TestRecordWriter(),
                               new NamingStrategy() );
        journal.close();
    }

    @Test
    public void loadShortenedJournal()
        throws Exception
    {
        File path = new File( "target/journals/loadShortenedJournal" );
        path.mkdirs();

        DiskJournal<TestRecord> journal =
            new DiskJournal<>( "loadShortenedJournal", path.toPath(), new FlushListener(), 1024 * 1024,
                               new RecordIdGenerator(), new TestRecordReader(), new TestRecordWriter(),
                               new NamingStrategy() );

        journal.appendEntry( buildTestRecord( 1, "test1", (byte) 12 ) );
        journal.appendEntry( buildTestRecord( 2, "test2", (byte) 24 ) );
        journal.appendEntry( buildTestRecord( 4, "test3", (byte) 32 ) );
        journal.appendEntry( buildTestRecord( 8, "test4", (byte) 48 ) );

        journal.close();

        File file = new File( path, "journal-1" );
        RandomAccessFile raf = new RandomAccessFile( file, "rws" );
        raf.setLength( 130 );
        raf.close();

        assertEquals( 130, file.length() );

        journal =
            new DiskJournal<>( "loadShortenedJournal", path.toPath(), new FlushListener(), 1024 * 1024,
                               new RecordIdGenerator(), new TestRecordReader(), new TestRecordWriter(),
                               new NamingStrategy() );
        journal.close();
    }

    private SimpleJournalEntry<TestRecord> buildTestRecord( int value, String name, byte type )
    {
        TestRecord record = new TestRecord();
        record.value = value;
        record.name = name;
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
                return new SimpleJournalEntry<BasicDiskJournalTest.TestRecord>( record, type );
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
        implements JournalFlushedListener<TestRecord>
    {

        @Override
        public void flushed( JournalEntry<TestRecord> entry )
        {
            System.out.println( "flushed: " + entry );
        }

        @Override
        public void failed( JournalEntry<TestRecord> entry, JournalException cause )
        {
            System.out.println( "failed: " + entry );
        }

    }

}
