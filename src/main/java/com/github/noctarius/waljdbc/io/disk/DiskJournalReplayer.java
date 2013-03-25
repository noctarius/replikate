package com.github.noctarius.waljdbc.io.disk;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.noctarius.waljdbc.JournalRecord;
import com.github.noctarius.waljdbc.exceptions.ReplayCancellationException;
import com.github.noctarius.waljdbc.spi.JournalEntryReader;
import com.github.noctarius.waljdbc.spi.JournalListener;
import com.github.noctarius.waljdbc.spi.ReplayNotificationResult;

class DiskJournalReplayer<V>
{

    private static final Logger LOGGER = LoggerFactory.getLogger( DiskJournalReplayer.class );

    private final DiskJournal<V> journal;

    private final JournalListener<V> listener;

    public DiskJournalReplayer( DiskJournal<V> journal, JournalListener<V> listener )
    {
        this.journal = journal;
        this.listener = listener;
    }

    public void replay()
    {
        List<DiskJournalRecord<V>> records = new LinkedList<>();
        List<DiskJournalFile<V>> diskJournalFiles = new LinkedList<>();

        File directory = journal.getJournalingPath().toFile();
        for ( File child : directory.listFiles() )
        {
            if ( child.isDirectory() )
                continue;

            String filename = child.getName();
            if ( journal.getNamingStrategy().isJournal( filename ) )
            {
                Tuple<DiskJournalFile<V>, List<DiskJournalRecord<V>>> result = readForward( child.toPath() );
                diskJournalFiles.add( result.getValue1() );
                records.addAll( result.getValue2() );
            }
        }

        Collections.sort( diskJournalFiles );
        Collections.sort( records );

        // Iterate the list and search for holes in history
        long lastRecordId = -1;
        JournalRecord<V> lastRecord = null;
        for ( DiskJournalRecord<V> record : records )
        {
            if ( lastRecordId != -1 && record.getRecordId() != lastRecordId + 1 )
            {
                LOGGER.error( "{}: There is a hole in history in journal {}->{}", journal.getName(), lastRecordId,
                              record.getRecordId() );

                try
                {
                    ReplayNotificationResult result =
                        listener.replayNotifySuspiciousRecordId( journal, lastRecord, record );
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
                    throw new ReplayCancellationException( "Replay of journal was aborted due "
                        + "to missing recordId in the journal file", e );
                }
            }
            lastRecordId = record.getRecordId();
            lastRecord = record;
        }

        // Push all found journal files to DiskJournal
        for ( DiskJournalFile<V> diskJournalFile : diskJournalFiles )
        {
            journal.pushJournalFileFromReplay( diskJournalFile );
        }

        for ( DiskJournalRecord<V> record : records )
        {
            LOGGER.info( "{}: Reannouncing journal entry  {}", journal.getName(), record.getRecordId() );
            try
            {
                ReplayNotificationResult result = listener.replayRecordId( journal, record );
                if ( result == ReplayNotificationResult.Except )
                {
                    throw new ReplayCancellationException( "Replay of journal was aborted by callback" );
                }
                else if ( result == ReplayNotificationResult.Terminate )
                {
                    journal.getRecordIdGenerator().notifyHighestJournalRecordId( record.getRecordId() );
                    break;
                }
                journal.getRecordIdGenerator().notifyHighestJournalRecordId( record.getRecordId() );
            }
            catch ( RuntimeException e )
            {
                throw new ReplayCancellationException( "Replay of journal was aborted due to exception in callback", e );
            }
        }
    }

    private Tuple<DiskJournalFile<V>, List<DiskJournalRecord<V>>> readForward( Path file )
    {
        DiskJournalFile<V> diskJournalFile = null;
        List<DiskJournalRecord<V>> records = new LinkedList<>();
        try ( RandomAccessFile raf = new RandomAccessFile( file.toFile(), "r" ) )
        {
            JournalFileHeader header = DiskJournalIOUtils.readHeader( raf );
            diskJournalFile = new DiskJournalFile<>( raf, header, journal );
            LOGGER.info( "{}: Reading old journal file with logNumber {}", journal.getName(), header.getLogNumber() );

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
                    LOGGER.debug( "pos={}, startingLength={}, endingLength={}", pos, startingLength, endingLength );
                    LOGGER.info( "{}: Incomplete record in journal file with logNumber {}", journal.getName(),
                                 header.getLogNumber() );
                    break;
                }

                LOGGER.debug( "{}: Found new record in logNumber {}", journal.getName(), header.getLogNumber() );

                // Read recordId
                raf.seek( pos + 4 );
                long recordId = raf.readLong();

                LOGGER.debug( "{}: Reading record {}", journal.getName(), recordId );

                // Read information of the entry
                byte type = raf.readByte();
                int entryLength = startingLength - DiskJournal.JOURNAL_RECORD_HEADER_SIZE;
                byte[] entryData = new byte[entryLength];
                raf.readFully( entryData );

                JournalEntryReader<V> reader = journal.getReader();
                records.add( new DiskJournalRecord<>( reader.readJournalEntry( recordId, type, entryData ), recordId,
                                                      journal, diskJournalFile ) );

                pos += startingLength;
            }
        }
        catch ( IOException e )
        {
            // Something went wrong but we want to execute as much journal entries as possible so we'll ignore that one
            // here!
        }
        return new Tuple<>( diskJournalFile, records );
    }

}
