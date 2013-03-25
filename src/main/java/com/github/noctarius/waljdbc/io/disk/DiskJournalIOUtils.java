package com.github.noctarius.waljdbc.io.disk;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.noctarius.waljdbc.spi.JournalEntryReader;

abstract class DiskJournalIOUtils
{

    private static final Logger LOGGER = LoggerFactory.getLogger( DiskJournalIOUtils.class );

    private DiskJournalIOUtils()
    {
    }

    static JournalFileHeader createJournal( RandomAccessFile raf, JournalFileHeader header )
        throws IOException
    {
        long nanoSeconds = System.nanoTime();
        byte[] prefiller = new byte[header.getMaxLogFileSize()];
        Arrays.fill( prefiller, (byte) 0 );

        // Resize the file to maxLogFileSize
        raf.setLength( header.getMaxLogFileSize() );
        raf.seek( 0 );

        try ( ByteArrayBufferOutputStream buffer = new ByteArrayBufferOutputStream( prefiller );
                        DataOutputStream stream = new DataOutputStream( buffer ) )
        {
            stream.write( JournalFileHeader.MAGIC_NUMBER );
            stream.writeInt( header.getVersion() );
            stream.writeInt( header.getMaxLogFileSize() );
            stream.writeLong( header.getLogNumber() );
            stream.write( header.getType() );
            stream.writeInt( header.getFirstDataOffset() );
        }

        raf.write( prefiller );
        raf.seek( DiskJournal.JOURNAL_FILE_HEADER_SIZE );

        LOGGER.trace( "DiskJournalIOUtils::createJournal took {}ns", ( System.nanoTime() - nanoSeconds ) );

        return header;
    }

    static JournalFileHeader readHeader( RandomAccessFile raf )
        throws IOException
    {
        // Read header and look for expected values
        byte[] magicNumber = new byte[4];
        raf.readFully( magicNumber );
        if ( !Arrays.equals( magicNumber, JournalFileHeader.MAGIC_NUMBER ) )
        {
            throw new IllegalStateException( "Given file no legal journal" );
        }

        int version = raf.readInt();
        int maxLogFileSize = raf.readInt();
        long logFileNumber = raf.readLong();
        byte type = raf.readByte();
        int firstDataOffset = raf.readInt();
        return new JournalFileHeader( version, maxLogFileSize, logFileNumber, type, firstDataOffset );
    }

    static <V> void writeRecord( DiskJournalRecord<V> record, byte[] entryData, RandomAccessFile raf )
        throws IOException
    {
        long nanoSeconds = System.nanoTime();

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

        LOGGER.trace( "DiskJournalIOUtils::writeRecord took {}ns", ( System.nanoTime() - nanoSeconds ) );
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
