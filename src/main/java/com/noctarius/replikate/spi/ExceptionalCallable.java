package com.noctarius.replikate.spi;

import java.util.concurrent.Callable;

public interface ExceptionalCallable<V>
        extends Callable<V> {

    V execute()
            throws Exception;

    default V call() {
        try {
            return execute();

        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        }
    }

}
