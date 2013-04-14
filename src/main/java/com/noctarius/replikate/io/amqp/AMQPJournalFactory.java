/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package com.noctarius.replikate.io.amqp;

import java.util.concurrent.ExecutorService;

import com.noctarius.replikate.Journal;
import com.noctarius.replikate.JournalConfiguration;
import com.noctarius.replikate.exceptions.JournalConfigurationException;
import com.noctarius.replikate.spi.JournalFactory;
import com.noctarius.replikate.spi.Preconditions;

public class AMQPJournalFactory<V>
    implements JournalFactory<V>
{

    @Override
    public Journal<V> buildJournal( String name, JournalConfiguration<V> configuration,
                                    ExecutorService listenerExecutorService )
    {
        Preconditions.checkType( configuration, AMQPJournalConfiguration.class, "configuration" );
        AMQPJournalConfiguration<V> config = (AMQPJournalConfiguration<V>) configuration;

        try
        {
            return new AMQPJournal<>( name, config.getAmqpUrl(), config.getRecordIdGenerator(),
                                      config.getEntryReader(), config.getEntryWriter(), config.getNamingStrategy(),
                                      listenerExecutorService );
        }
        catch ( Exception e )
        {
            throw new JournalConfigurationException( "Error while configuring the journal", e );
        }
    }

}
