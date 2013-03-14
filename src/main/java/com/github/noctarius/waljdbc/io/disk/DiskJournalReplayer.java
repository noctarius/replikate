package com.github.noctarius.waljdbc.io.disk;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.noctarius.waljdbc.JournalEntry;
import com.github.noctarius.waljdbc.exceptions.ReplayCancellationException;
import com.github.noctarius.waljdbc.spi.JournalEntryReader;
import com.github.noctarius.waljdbc.spi.JournalFlushedListener;
import com.github.noctarius.waljdbc.spi.ReplayNotificationResult;

class DiskJournalReplayer<V>
{

    private static final Logger LOGGER = LoggerFactory.getLogger( DiskJournalReplayer.class );

    private final DiskJournal<V> journal;

    private final JournalFlushedListener<V> listener;

    public DiskJournalReplayer( DiskJournal<V> journal, JournalFlushedListener<V> listener )
    {
        this.journal = journal;
        this.listener = listener;
    }

    public void replay()
    {
        List<DiskJournalRecord<V>> records = new LinkedList<>();

        File directory = journal.getJournalingPath().toFile();
        for ( File child : directory.listFiles() )
        {
            if ( child.isDirectory() )
                continue;

            String filename = child.getName();
            if ( journal.getNamingStrategy().isJournal( filename ) )
            {
                records.addAll( readForward( child.toPath() ) );
            }
        }

        Collections.sort( records );

        // Iterate the list and search for holes in history
        long lastRecordId = -1;
        JournalEntry<V> lastRecord = null;
        for ( DiskJournalRecord<V> record : records )
        {
            if ( lastRecordId != -1 && record.getRecordId() != lastRecordId + 1 )
            {
                LOGGER.error( journal.getName() + ": There is a hole in history in journal " + lastRecordId + "->"
                    + record.getRecordId() );

                try
                {
                    ReplayNotificationResult result =
                        listener.replayNotifySuspiciousRecordId( journal, lastRecord, record.getJournalEntry() );
                    if ( result == ReplayNotificationResult.Except )
                    {
                        throw new ReplayCancellationException( "Replay of journal was aborted due by callback" );
                    }
                    else if ( result == ReplayNotificationResult.Terminate )
                    {
                        break;
                    }
                }
                catch ( RuntimeException e )
                {
                    throw new ReplayCancellationException( "Replay of journal was aborted due "
                        + "to missing recordId in the journal file", e );
                }
            }
            lastRecordId = record.getRecordId();
            lastRecord = record.getJournalEntry();
        }

        for ( DiskJournalRecord<V> record : records )
        {
            LOGGER.info( journal.getName() + ": Reannouncing journal entry " + record.getRecordId() );
            try
            {
                ReplayNotificationResult result = listener.replayRecordId( journal, record.getJournalEntry() );
                if ( result == ReplayNotificationResult.Except )
                {
                    throw new ReplayCancellationException( "Replay of journal was aborted by callback" );
                }
                else if ( result == ReplayNotificationResult.Terminate )
                {
                    break;
                }
            }
            catch ( RuntimeException e )
            {
                throw new ReplayCancellationException( "Replay of journal was aborted due to exception in callback", e );
            }
        }
    }

    private List<DiskJournalRecord<V>> readForward( Path file )
    {
        List<DiskJournalRecord<V>> records = new LinkedList<>();
        try ( RandomAccessFile raf = new RandomAccessFile( file.toFile(), "r" ) )
        {
            JournalFileHeader header = readHeader( raf );
            LOGGER.info( journal.getName() + ": Reading old journal file with logNumber " + header.getLogNumber() );

            int pos = header.getFirstDataOffset();
            while ( pos < raf.length() )
            {
                // Read length at begin of the record start
                raf.seek( pos );
                int startingLength = raf.readInt();

                if ( startingLength == 0 )
                {
                    // File is completely read
                    break;
                }

                // Read length at begin of the record end
                raf.seek( pos + startingLength - 4 );
                int endingLength = raf.readInt();

                // If both length values differ this record is broken
                if ( startingLength != endingLength )
                {
                    LOGGER.debug( "pos=" + pos + ", startingLength=" + startingLength + ", endingLength="
                        + endingLength );
                    LOGGER.info( journal.getName() + ": Incomplete record in journal file with logNumber "
                        + header.getLogNumber() );
                    break;
                }

                LOGGER.debug( journal.getName() + ": Found new record in logNumber " + header.getLogNumber() );

                // Read recordId
                raf.seek( pos + 4 );
                long recordId = raf.readLong();

                LOGGER.debug( journal.getName() + ": Reading record " + recordId );

                // Read information of the entry
                byte type = raf.readByte();
                int entryLength = startingLength - DiskJournal.JOURNAL_RECORD_HEADER_SIZE;
                byte[] entryData = new byte[entryLength];
                raf.readFully( entryData );

                JournalEntryReader<V> reader = journal.getReader();
                records.add( new DiskJournalRecord<>( reader.readJournalEntry( recordId, type, entryData ), recordId ) );

                pos += startingLength;
            }
        }
        catch ( IOException e )
        {
            // Something went wrong but we want to execute as much journal entries as possible so we'll ignore that one
            // here!
        }
        return records;
    }

    private JournalFileHeader readHeader( RandomAccessFile raf )
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

}
