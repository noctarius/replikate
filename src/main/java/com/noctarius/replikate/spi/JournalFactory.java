package com.noctarius.replikate.spi;

import java.util.concurrent.ExecutorService;

import com.noctarius.replikate.Journal;
import com.noctarius.replikate.JournalConfiguration;

public interface JournalFactory<V>
{

    Journal<V> buildJournal( String name, JournalConfiguration<V> configuration, ExecutorService listenerExecutorService );

}
