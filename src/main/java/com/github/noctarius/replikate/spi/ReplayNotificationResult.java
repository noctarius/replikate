package com.github.noctarius.replikate.spi;

import com.github.noctarius.replikate.exceptions.JournalException;

public enum ReplayNotificationResult
{

    /** Silently stops the execution */
    Terminate,

    /** Throws a {@link JournalException} and prevents further execution */
    Except,

    /** Executes / Reads the next record */
    Continue

}
