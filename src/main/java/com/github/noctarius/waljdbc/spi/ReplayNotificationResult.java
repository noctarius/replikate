package com.github.noctarius.waljdbc.spi;

import com.github.noctarius.waljdbc.exceptions.JournalException;

public enum ReplayNotificationResult
{

    /** Silently stops the execution */
    Terminate,

    /** Throws a {@link JournalException} and prevents further execution */
    Except,

    /** Executes / Reads the next record */
    Continue

}
