package com.github.noctarius.replikate.spi;

import java.util.concurrent.ExecutorService;

import com.github.noctarius.replikate.Journal;
import com.github.noctarius.replikate.JournalConfiguration;

public interface JournalFactory<V>
{

    Journal<V> buildJournal( String name, JournalConfiguration<V> configuration, ExecutorService listenerExecutorService );

}
