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

import com.noctarius.replikate.exceptions.JournalException;
import com.noctarius.replikate.spi.ReplayNotificationResult;

public interface JournalListener<V> {

    void onCommit(JournalRecord<V> record);

    void onFailure(JournalEntry<V> entry, JournalException cause);

    void onFailure(JournalBatch<V> journalBatch, JournalException cause);

    /**
     * <p>This callback method is called when during journal replaying a missing recordId or other suspicious events arise.</p>
     * The further behavior of the journal replay can be influenced using the callback's result.
     * <ul>
     * <li>{@link ReplayNotificationResult#Continue} means that execution will be move on with the next record or finish
     * initialization of journal when replay is finished</li>
     * <li>{@link ReplayNotificationResult#Terminate} will silently finish the record reading and will only announce the
     * already read entries</li>
     * <li>Finally {@link ReplayNotificationResult#Except} will throw a {@link JournalException} and none of the already
     * read entries will be announced (same can be achieved by throwing an {@link RuntimeException} inside the callback)
     * </li>
     * </ul>
     *
     * @param journal       The journal that is read
     * @param lastRecord    The previously read journal record
     * @param currentRecord The currently read journal record
     * @return Further execution behavior
     */
    ReplayNotificationResult onReplaySuspiciousRecordId(Journal<V> journal, JournalRecord<V> lastRecord,
                                                        JournalRecord<V> currentRecord);

    /**
     * <p>This method is called when during startup of the Journal implementation a previous journal was found and records
     * replayed. For every record in the existing journal the callback will be executed.</p>
     * The further behavior of the journal replay can be influenced using the callback's result.
     * <ul>
     * <li>{@link ReplayNotificationResult#Continue} means that execution will be move on with the next record or finish
     * initialization of journal when replay is finished</li>
     * <li>{@link ReplayNotificationResult#Terminate} will silently finish the record reading</li>
     * <li>{@link ReplayNotificationResult#Except} will throw a {@link JournalException} (same can be achieved by
     * throwing an {@link RuntimeException} inside the callback)</li>
     * </ul>
     *
     * @param journal The journal that is read
     * @param record  The currently read journal record
     * @return Further execution behavior
     */
    ReplayNotificationResult onReplayRecordId(Journal<V> journal, JournalRecord<V> record);

}
