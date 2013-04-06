package com.github.noctarius.replikate.spi;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory
    implements ThreadFactory
{

    private final AtomicInteger count = new AtomicInteger( 0 );

    private final String prefix;

    public NamedThreadFactory( String prefix )
    {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread( Runnable r )
    {
        return new Thread( r, prefix + "-" + ( count.getAndIncrement() ) );
    }

}
