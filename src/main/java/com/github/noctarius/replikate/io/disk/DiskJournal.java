package com.github.noctarius.replikate.io.disk;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.noctarius.replikate.JournalEntry;
import com.github.noctarius.replikate.JournalListener;
import com.github.noctarius.replikate.JournalNamingStrategy;
import com.github.noctarius.replikate.JournalRecord;
import com.github.noctarius.replikate.exceptions.JournalException;
import com.github.noctarius.replikate.exceptions.SynchronousJournalException;
import com.github.noctarius.replikate.spi.AbstractJournal;
import com.github.noctarius.replikate.spi.JournalEntryReader;
import com.github.noctarius.replikate.spi.JournalEntryWriter;
import com.github.noctarius.replikate.spi.JournalRecordIdGenerator;

public class DiskJournal<V>
    extends AbstractJournal<V>
{

    public static final int JOURNAL_FILE_HEADER_SIZE = 25;

    public static final int JOURNAL_RECORD_HEADER_SIZE = 17;

    public static final int JOURNAL_OVERFLOW_OVERHEAD_SIZE = JOURNAL_FILE_HEADER_SIZE + JOURNAL_RECORD_HEADER_SIZE;

    public static final byte JOURNAL_FILE_TYPE_DEFAULT = 1;

    public static final byte JOURNAL_FILE_TYPE_OVERFLOW = 2;

    private static final Logger LOGGER = LoggerFactory.getLogger( DiskJournal.class );

    private final Deque<DiskJournalFile<V>> journalFiles = new ConcurrentLinkedDeque<>();

    private final JournalListener<V> listener;

    private final Path journalingPath;

    public DiskJournal( String name, Path journalingPath, JournalListener<V> listener, int maxLogFileSize,
                        JournalRecordIdGenerator recordIdGenerator, JournalEntryReader<V> reader,
                        JournalEntryWriter<V> writer, JournalNamingStrategy namingStrategy )
        throws IOException
    {
        super( name, maxLogFileSize, recordIdGenerator, reader, writer, namingStrategy );
        this.journalingPath = journalingPath;
        this.listener = listener;

        if ( !Files.isDirectory( journalingPath, LinkOption.NOFOLLOW_LINKS ) )
        {
            throw new IllegalArgumentException( "journalingPath is not a directory" );
        }

        LOGGER.info( "{}: DiskJournal starting up in {}...", getName(), journalingPath.toFile().getAbsolutePath() );

        boolean needsReplay = false;
        File path = journalingPath.toFile();
        for ( File child : path.listFiles() )
        {
            if ( child.isDirectory() )
                continue;

            String filename = child.getName();
            if ( namingStrategy.isJournal( filename ) )
            {
                // At least one journal file is still existing so start replay
                needsReplay = true;
                break;
            }
        }

        if ( needsReplay )
        {
            LOGGER.warn( "{}: Found old journals in journaling path, starting replay...", getName() );
            DiskJournalReplayer<V> replayer = new DiskJournalReplayer<>( this, listener );
            replayer.replay();
        }

        // Replay not required or succeed so start new journal
        journalFiles.push( buildJournalFile() );
    }

    @Override
    public void appendEntry( JournalEntry<V> entry )
        throws JournalException
    {
        appendEntry( entry, listener );
    }

    @Override
    public void appendEntry( JournalEntry<V> entry, JournalListener<V> listener )
    {
        try
        {
            synchronized ( journalFiles )
            {
                DiskJournalFile<V> journalFile = journalFiles.peek();
                DiskJournalEntryFacade<V> recordEntry = new DiskJournalEntryFacade<>( entry );
                Tuple<DiskJournalAppendResult, JournalRecord<V>> result = journalFile.appendRecord( recordEntry );
                if ( result.getValue1() == DiskJournalAppendResult.APPEND_SUCCESSFUL )
                {
                    if ( listener != null )
                    {
                        listener.onSync( result.getValue2() );
                    }
                }
                else if ( result.getValue1() == DiskJournalAppendResult.JOURNAL_OVERFLOW )
                {
                    LOGGER.debug( "Journal full, overflowing to next one..." );

                    // Close current journal file ...
                    journalFile.close();

                    // ... and start new journal ...
                    journalFiles.push( buildJournalFile() );

                    // ... finally retry to write to journal
                    appendEntry( entry, listener );
                }
                else if ( result.getValue1() == DiskJournalAppendResult.JOURNAL_FULL_OVERFLOW )
                {
                    LOGGER.debug( "Record dataset too large for normal journal, using overflow journal file" );

                    // Close current journal file ...
                    journalFile.close();

                    // Calculate overflow filelength
                    int length = recordEntry.cachedData.length + DiskJournal.JOURNAL_OVERFLOW_OVERHEAD_SIZE;

                    // ... and start new journal ...
                    journalFile = buildJournalFile( length, DiskJournal.JOURNAL_FILE_TYPE_OVERFLOW );
                    journalFiles.push( journalFile );

                    // ... finally retry to write to journal
                    result = journalFile.appendRecord( recordEntry );
                    if ( result.getValue1() != DiskJournalAppendResult.APPEND_SUCCESSFUL )
                    {
                        throw new SynchronousJournalException( "Overflow file could not be written" );
                    }

                    // Notify listeners about flushed to journal
                    if ( listener != null )
                    {
                        listener.onSync( result.getValue2() );
                    }
                }
            }
        }
        catch ( IOException e )
        {
            if ( listener != null )
            {
                listener.onFailure( entry, new SynchronousJournalException( "Failed to persist journal entry", e ) );
            }
        }
    }

    @Override
    public long getLastRecordId()
    {
        return getRecordIdGenerator().lastGeneratedRecordId();
    }

    @Override
    public void close()
        throws IOException
    {
        synchronized ( journalFiles )
        {
            for ( DiskJournalFile<V> journalFile : journalFiles )
            {
                journalFile.close();
            }
        }
    }

    public Path getJournalingPath()
    {
        return journalingPath;
    }

    void pushJournalFileFromReplay( DiskJournalFile<V> diskJournalFile )
    {
        synchronized ( journalFiles )
        {
            journalFiles.push( diskJournalFile );
            setCurrentLogNumber( diskJournalFile.getLogNumber() );
        }
    }

    void journalRecordCommitted( DiskJournalRecord<V> record, DiskJournalFile<V> journalFile )
    {
        // TODO: Implementation missing
    }

    private DiskJournalFile<V> buildJournalFile()
        throws IOException
    {
        return buildJournalFile( getMaxLogFileSize(), DiskJournal.JOURNAL_FILE_TYPE_DEFAULT );
    }

    private DiskJournalFile<V> buildJournalFile( int maxLogFileSize, byte type )
        throws IOException
    {
        long logNumber = nextLogNumber();
        String filename = getNamingStrategy().generate( logNumber );
        File journalFile = new File( journalingPath.toFile(), filename );
        return new DiskJournalFile<>( this, journalFile, logNumber, maxLogFileSize, type );
    }

}
