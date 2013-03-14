package com.github.noctarius.waljdbc.io.disk;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.noctarius.waljdbc.JournalEntry;
import com.github.noctarius.waljdbc.exceptions.JournalException;
import com.github.noctarius.waljdbc.exceptions.SynchronousJournalException;
import com.github.noctarius.waljdbc.spi.AbstractJournal;
import com.github.noctarius.waljdbc.spi.JournalEntryReader;
import com.github.noctarius.waljdbc.spi.JournalEntryWriter;
import com.github.noctarius.waljdbc.spi.JournalFlushedListener;
import com.github.noctarius.waljdbc.spi.JournalNamingStrategy;
import com.github.noctarius.waljdbc.spi.JournalRecordIdGenerator;

public class DiskJournal<V>
    extends AbstractJournal<V>
{

    public static final int JOURNAL_FILE_HEADER_SIZE = 25;

    public static final int JOURNAL_RECORD_HEADER_SIZE = 17;

    public static final byte JOURNAL_FILE_TYPE_DEFAULT = 1;

    public static final byte JOURNAL_FILE_TYPE_OVERFLOW = 2;

    private static final Logger LOGGER = LoggerFactory.getLogger( DiskJournal.class );

    private final AtomicReference<DiskJournalFile<V>> journalFile = new AtomicReference<>();

    private final JournalFlushedListener<V> listener;

    private final Path journalingPath;

    public DiskJournal( String name, Path journalingPath, JournalFlushedListener<V> listener, int maxLogFileSize,
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

        LOGGER.info( getName() + ": DiskJournal starting up in " + journalingPath.toFile().getAbsolutePath() + "..." );

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
            LOGGER.warn( getName() + ": Found old journals in journaling path, starting replay..." );
            DiskJournalReplayer<V> replayer = new DiskJournalReplayer<>( this, listener );
            replayer.replay();
        }

        // Replay not required or succeed so start new journal
        journalFile.set( buildJournalFile() );
    }

    @Override
    public void appendEntry( JournalEntry<V> entry )
        throws JournalException
    {
        appendEntry( entry, listener );
    }

    @Override
    public void appendEntry( JournalEntry<V> entry, JournalFlushedListener<V> listener )
    {
        try
        {
            journalFile.get().appendRecord( entry );
            if ( listener != null )
            {
                listener.flushed( entry );
            }
        }
        catch ( IOException e )
        {
            if ( listener != null )
            {
                listener.failed( entry, new SynchronousJournalException( "Failed to persist journal entry", e ) );
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
        journalFile.get().close();
    }

    public Path getJournalingPath()
    {
        return journalingPath;
    }

    private DiskJournalFile<V> buildJournalFile()
        throws IOException
    {
        long logNumber = nextLogNumber();
        String filename = getNamingStrategy().generate( logNumber );
        File journalFile = new File( journalingPath.toFile(), filename );
        return new DiskJournalFile<>( this, journalFile, logNumber );
    }

}
