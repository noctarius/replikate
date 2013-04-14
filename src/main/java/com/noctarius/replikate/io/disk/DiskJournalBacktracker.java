package com.noctarius.replikate.io.disk;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.noctarius.replikate.spi.JournalEntryReader;

class DiskJournalBacktracker<V>
{

    private static final Logger LOGGER = LoggerFactory.getLogger( DiskJournalBacktracker.class );

    private final DiskJournal<V> journal;

    private final Path journalingPath;

    DiskJournalBacktracker( DiskJournal<V> journal, Path journalingPath )
    {
        this.journal = journal;
        this.journalingPath = journalingPath;
    }

    public synchronized DiskJournalFile<V> backtrack( long lastRecordId )
        throws IOException
    {
        BacktrackingFileWalker fileWalker = new BacktrackingFileWalker( lastRecordId );
        Files.walkFileTree( journalingPath, fileWalker );
        return fileWalker.foundJournalFile;
    }

    private class BacktrackingFileWalker
        extends SimpleFileVisitor<Path>
    {

        private final long lastRecordId;

        private final byte[] data = new byte[journal.getMaxLogFileSize()];

        private DiskJournalFile<V> foundJournalFile = null;

        private BacktrackingFileWalker( long lastRecordId )
        {
            this.lastRecordId = lastRecordId;
        }

        @Override
        public FileVisitResult visitFile( Path file, BasicFileAttributes attrs )
            throws IOException
        {
            RandomAccessFile raf = new RandomAccessFile( file.toFile(), "r" );
            if ( raf.length() < journal.getMaxLogFileSize() )
            {
                LOGGER.warn( "{}: Incomplete journal found, reading as much as possible...", journal.getName() );
            }

            // Is file completely full we need to start searching at the beginning because we cannot guarantee that the
            // files end is not broken
            DiskJournalRecord<V> record = forwardSearch( raf );
            if ( record.getRecordId() == lastRecordId )
            {
                long logNumber = journal.getNamingStrategy().extractLogNumber( file.toFile().getName() );
                foundJournalFile = new DiskJournalFile<V>( journal, file.toFile(), logNumber, -1, (byte) -1, false );
                return FileVisitResult.TERMINATE;
            }

            return FileVisitResult.CONTINUE;
        }

        private DiskJournalRecord<V> forwardSearch( RandomAccessFile raf )
            throws IOException
        {
            // Start directly after the header
            raf.seek( DiskJournal.JOURNAL_FILE_HEADER_SIZE );

            int pos = DiskJournal.JOURNAL_FILE_HEADER_SIZE;
            while ( pos < data.length )
            {
                // Read length at begin of the record start
                raf.seek( pos );
                int startingLength = raf.readInt();

                // Read length at begin of the record end
                raf.seek( pos + startingLength - 4 );
                int endingLength = raf.readInt();

                // If both length values differ this record is broken
                if ( startingLength != endingLength )
                {
                    return null;
                }

                // Read recordId
                raf.seek( pos + 4 );
                long recordId = raf.readLong();

                // Return this record if this is the last applied recordId
                if ( recordId == lastRecordId )
                {
                    // Read information of the entry
                    byte type = raf.readByte();
                    int entryLength = startingLength - DiskJournal.JOURNAL_RECORD_HEADER_SIZE;
                    byte[] entryData = new byte[entryLength];
                    raf.readFully( entryData );

                    JournalEntryReader<V> reader = journal.getReader();
                    return new DiskJournalRecord<>( reader.readJournalEntry( recordId, type, entryData ), recordId );
                }

                pos += startingLength;
            }

            return null;
        }
    }

}
