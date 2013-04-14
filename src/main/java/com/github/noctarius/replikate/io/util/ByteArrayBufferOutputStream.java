package com.github.noctarius.replikate.io.util;

import java.io.IOException;
import java.io.OutputStream;

public class ByteArrayBufferOutputStream
    extends OutputStream
{

    private final byte[] wrapped;

    private int pos = 0;

    public ByteArrayBufferOutputStream( byte[] wrapped )
    {
        this.wrapped = wrapped;
    }

    @Override
    public void write( int b )
        throws IOException
    {
        if ( pos == wrapped.length )
        {
            throw new ArrayIndexOutOfBoundsException( "Position " + pos + " is outside of wrapped array" );
        }

        wrapped[pos++] = (byte) b;
    }

}
