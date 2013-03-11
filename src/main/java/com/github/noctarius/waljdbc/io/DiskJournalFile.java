package com.github.noctarius.waljdbc.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

public class DiskJournalFile
{

    private final RandomAccessFile raf;

    DiskJournalFile( File file )
        throws FileNotFoundException
    {
        raf = new RandomAccessFile( file, "rws" );
    }

}
