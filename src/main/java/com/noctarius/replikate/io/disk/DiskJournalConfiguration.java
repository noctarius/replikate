package com.noctarius.replikate.io.disk;

import java.nio.file.Path;

import com.noctarius.replikate.JournalConfiguration;

public class DiskJournalConfiguration<V>
    extends JournalConfiguration<V>
{

    private Path journalingPath;

    private int maxLogFileSize;

    public Path getJournalingPath()
    {
        return journalingPath;
    }

    public void setJournalingPath( Path journalingPath )
    {
        this.journalingPath = journalingPath;
    }

    public int getMaxLogFileSize()
    {
        return maxLogFileSize;
    }

    public void setMaxLogFileSize( int maxLogFileSize )
    {
        this.maxLogFileSize = maxLogFileSize;
    }

}
