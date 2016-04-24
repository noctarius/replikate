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
package com.noctarius.replikate;

import com.noctarius.replikate.spi.JournalFactory;
import com.noctarius.replikate.spi.SimpleJournalSystem;

import java.util.concurrent.ExecutorService;

public interface JournalSystem {

    <V> Journal<V> getJournal(String name, JournalConfiguration<V> configuration);

    <V> Journal<V> getJournal(String name, JournalFactory<V> journalFactory, JournalConfiguration<V> configuration);

    <V> Journal<V> getJournal(String name, JournalStrategy journalStrategy, JournalConfiguration<V> configuration);

    void shutdown();

    static JournalSystem newJournalSystem() {
        return newJournalSystem(null);
    }

    static JournalSystem newJournalSystem(ExecutorService listenerExecutorService) {
        return new SimpleJournalSystem(listenerExecutorService);
    }

}
