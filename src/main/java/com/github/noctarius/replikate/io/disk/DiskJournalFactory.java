package com.github.noctarius.replikate.io.disk;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import com.github.noctarius.replikate.Journal;
import com.github.noctarius.replikate.JournalConfiguration;
import com.github.noctarius.replikate.exceptions.JournalConfigurationException;
import com.github.noctarius.replikate.spi.JournalFactory;
import com.github.noctarius.replikate.spi.Preconditions;

public class DiskJournalFactory<V>
    implements JournalFactory<V>
{

    public static final JournalFactory<?> DEFAULT_INSTANCE = new DiskJournalFactory<>();

    private static final int MIN_DISKJOURNAL_FILE_SIZE = 1024 * 1024;

    @Override
    public Journal<V> buildJournal( String name, JournalConfiguration<V> configuration,
                                    ExecutorService listenerExecutorService )
    {
        Preconditions.checkType( configuration, DiskJournalConfiguration.class, "configuration" );
        DiskJournalConfiguration<V> diskConfig = (DiskJournalConfiguration<V>) configuration;

        Preconditions.notNull( diskConfig.getJournalingPath(), "configuration.journalingPath" );

        if ( diskConfig.getMaxLogFileSize() < MIN_DISKJOURNAL_FILE_SIZE )
        {
            throw new IllegalArgumentException( "configuration.maxLogFileSize must not be below "
                + MIN_DISKJOURNAL_FILE_SIZE );
        }

        try
        {
            return new DiskJournal<>( name, diskConfig.getJournalingPath(), diskConfig.getListener(),
                                      diskConfig.getMaxLogFileSize(), diskConfig.getRecordIdGenerator(),
                                      diskConfig.getEntryReader(), diskConfig.getEntryWriter(),
                                      diskConfig.getNamingStrategy(), listenerExecutorService );
        }
        catch ( IOException e )
        {
            throw new JournalConfigurationException( "Error while configuring the journal", e );
        }
    }
}