package com.github.noctarius.replikate.spi;

import com.github.noctarius.replikate.Journal;
import com.github.noctarius.replikate.JournalEntry;
import com.github.noctarius.replikate.JournalRecord;
import com.github.noctarius.replikate.exceptions.JournalException;

public interface JournalListener<V>
{

    void onFlushed( JournalRecord<V> record );

    void onFailed( JournalEntry<V> entry, JournalException cause );

    /**
     * This callback method is called when during journal replaying a missing recordId or other suspicious events arise.<br>
     * The further behavior of the journal replay can be influenced using the callback's result.<br>
     * <ul>
     * <li>{@link ReplayNotificationResult#Continue} means that execution will be move on with the next record or finish
     * initialization of journal when replay is finished</li><br>
     * <li>{@link ReplayNotificationResult#Terminate} will silently finish the record reading and will only announce the
     * already read entries</li><br>
     * <li>Finally {@link ReplayNotificationResult#Except} will throw a {@link JournalException} and none of the already
     * read entries will be announced (same can be achieved by throwing an {@link RuntimeException} inside the callback)
     * </li>
     * </ul>
     * 
     * @param journal The journal that is read
     * @param lastRecord The previously read journal record
     * @param currentRecord The currently read journal record
     * @return Further execution behavior
     */
    ReplayNotificationResult onReplaySuspiciousRecordId( Journal<V> journal, JournalRecord<V> lastRecord,
                                                             JournalRecord<V> currentRecord );

    /**
     * This method is called when during startup of the Journal implementation a previous journal was found and records
     * replayed. For every record in the existing journal the callback will be executed.<br>
     * The further behavior of the journal replay can be influenced using the callback's result.<br>
     * <ul>
     * <li>{@link ReplayNotificationResult#Continue} means that execution will be move on with the next record or finish
     * initialization of journal when replay is finished</li><br>
     * <li>{@link ReplayNotificationResult#Terminate} will silently finish the record reading</li><br>
     * <li>{@link ReplayNotificationResult#Except} will throw a {@link JournalException} (same can be achieved by
     * throwing an {@link RuntimeException} inside the callback)</li>
     * 
     * @param journal The journal that is read
     * @param record The currently read journal record
     * @return Further execution behavior
     */
    ReplayNotificationResult onReplayRecordId( Journal<V> journal, JournalRecord<V> record );

}
