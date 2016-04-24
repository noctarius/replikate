package com.noctarius.replikate.spi;

public interface ExceptionalRunnable
        extends Runnable {

    void execute()
            throws Exception;

    default void run() {
        try {
            execute();

        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        }
    }

}
