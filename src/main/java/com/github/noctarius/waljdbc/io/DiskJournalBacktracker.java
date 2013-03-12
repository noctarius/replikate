package com.github.noctarius.waljdbc.io;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;

import com.github.noctarius.waljdbc.spi.JournalEntryReader;

class DiskJournalBacktracker<V>
{

    private final DiskJournal<V> journal;

    private final Path journalingPath;

    public DiskJournalBacktracker( DiskJournal<V> journal, Path journalingPath )
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
            File journalFile = file.toFile();
            int fileLength = (int) journalFile.length();

            byte[] data = this.data;
            if ( fileLength != journal.getMaxLogFileSize() )
            {
                // Journal file has wrong size (not completely written?)
                data = new byte[fileLength];
            }

            InputStream is = Files.newInputStream( file, StandardOpenOption.READ );
            is.read( data );

            // Is file completely full we need to start searching at the beginning because we cannot guarantee that the
            // files end is not broken
            DiskJournalRecord<V> record = forwardSearch( data );
            if ( record.getRecordId() == lastRecordId )
            {
                foundJournalFile = new DiskJournalFile<V>( journal, journalFile, false );
                return FileVisitResult.TERMINATE;
            }

            return FileVisitResult.CONTINUE;
        }

        private DiskJournalRecord<V> forwardSearch( byte[] data )
            throws IOException
        {
            try ( DataByteArrayInputBuffer buffer = new DataByteArrayInputBuffer( data ) )
            {
                // Start directly after the header
                buffer.pos( DiskJournal.JOURNAL_FILE_HEADER_SIZE );

                int pos = DiskJournal.JOURNAL_FILE_HEADER_SIZE;
                while ( pos > data.length )
                {
                    // Read length at begin of the record start
                    buffer.pos( pos );
                    int startingLength = buffer.readInt();

                    // Read length at begin of the record end
                    buffer.pos( pos + startingLength - 4 );
                    int endingLength = buffer.readInt();

                    // If both length values differ this record is broken
                    if ( startingLength != endingLength )
                    {
                        return null;
                    }

                    // Read recordId
                    buffer.pos( pos + 4 );
                    long recordId = buffer.readLong();

                    // Return this record if this is the last applied recordId
                    if ( recordId == lastRecordId )
                    {
                        // Read information of the entry
                        byte type = buffer.readByte();
                        int entryLength = startingLength - DiskJournal.JOURNAL_RECORD_HEADER_SIZE;
                        byte[] entryData = new byte[entryLength];
                        buffer.readFully( entryData );

                        JournalEntryReader<V> reader = journal.getReader();
                        return new DiskJournalRecord<>( reader.readJournalEntry( recordId, type, entryData ), recordId );
                    }

                    pos += startingLength;
                }
            }

            return null;
        }
    }

}
