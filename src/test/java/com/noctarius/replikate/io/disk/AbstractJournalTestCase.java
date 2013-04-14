package com.noctarius.replikate.io.disk;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import com.noctarius.replikate.JournalConfiguration;
import com.noctarius.replikate.JournalListener;
import com.noctarius.replikate.JournalNamingStrategy;
import com.noctarius.replikate.io.disk.DiskJournalConfiguration;
import com.noctarius.replikate.spi.JournalEntryReader;
import com.noctarius.replikate.spi.JournalEntryWriter;
import com.noctarius.replikate.spi.JournalRecordIdGenerator;

public abstract class AbstractJournalTestCase
{

    protected File prepareJournalDirectory( String name )
        throws IOException
    {
        File path = new File( "target/journals/" + name );
        if ( path.exists() && path.isDirectory() )
        {
            Files.walkFileTree( path.toPath(), new SimpleFileVisitor<Path>()
            {

                @Override
                public FileVisitResult visitFile( Path file, BasicFileAttributes attrs )
                    throws IOException
                {
                    Files.delete( file );
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory( Path dir, IOException exc )
                    throws IOException
                {
                    Files.delete( dir );
                    return FileVisitResult.CONTINUE;
                }
            } );
        }

        path.mkdirs();
        return path;
    }

    protected <V> JournalConfiguration<V> buildDiskJournalConfiguration( Path journalingPath, int maxLogFileSize,
                                                                         JournalEntryReader<V> reader,
                                                                         JournalEntryWriter<V> writer,
                                                                         JournalListener<V> listener,
                                                                         JournalNamingStrategy namingStrategy,
                                                                         JournalRecordIdGenerator recordIdGenerator )
    {
        DiskJournalConfiguration<V> configuration = new DiskJournalConfiguration<>();
        configuration.setEntryReader( reader );
        configuration.setEntryWriter( writer );
        configuration.setJournalingPath( journalingPath );
        configuration.setListener( listener );
        configuration.setMaxLogFileSize( maxLogFileSize );
        configuration.setNamingStrategy( namingStrategy );
        configuration.setRecordIdGenerator( recordIdGenerator );

        return configuration;
    }

}
