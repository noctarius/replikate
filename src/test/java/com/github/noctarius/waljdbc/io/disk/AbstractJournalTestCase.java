package com.github.noctarius.waljdbc.io.disk;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

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
}
