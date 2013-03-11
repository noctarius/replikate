package com.github.noctarius.waljdbc.io;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.IOException;

public class DataByteArrayOutputStream
    extends ByteArrayOutputStream
    implements DataOutput
{

    DataByteArrayOutputStream()
    {
        super( 32 );
    }

    DataByteArrayOutputStream( int size )
    {
        super( size );
    }

    @Override
    public void writeBoolean( boolean v )
        throws IOException
    {
        write( v ? (byte) 1 : (byte) 0 );
    }

    @Override
    public void writeByte( int v )
        throws IOException
    {
        write( (byte) v );
    }

    @Override
    public void writeShort( int v )
        throws IOException
    {
        write( (byte) ( 0xff & ( v >> 8 ) ) );
        write( (byte) ( 0xff & v ) );
    }

    @Override
    public void writeChar( int v )
        throws IOException
    {
        write( (byte) ( 0xff & ( v >> 8 ) ) );
        write( (byte) ( 0xff & v ) );
    }

    @Override
    public void writeInt( int v )
        throws IOException
    {
        write( (byte) ( 0xff & ( v >> 24 ) ) );
        write( (byte) ( 0xff & ( v >> 16 ) ) );
        write( (byte) ( 0xff & ( v >> 8 ) ) );
        write( (byte) ( 0xff & v ) );
    }

    @Override
    public void writeLong( long v )
        throws IOException
    {
        write( (byte) ( 0xff & ( v >> 56 ) ) );
        write( (byte) ( 0xff & ( v >> 48 ) ) );
        write( (byte) ( 0xff & ( v >> 40 ) ) );
        write( (byte) ( 0xff & ( v >> 32 ) ) );
        write( (byte) ( 0xff & ( v >> 24 ) ) );
        write( (byte) ( 0xff & ( v >> 16 ) ) );
        write( (byte) ( 0xff & ( v >> 8 ) ) );
        write( (byte) ( 0xff & v ) );
    }

    @Override
    public void writeFloat( float v )
        throws IOException
    {
        writeInt( Float.floatToIntBits( v ) );
    }

    @Override
    public void writeDouble( double v )
        throws IOException
    {
        writeLong( Double.doubleToLongBits( v ) );
    }

    @Override
    public void writeBytes( String s )
        throws IOException
    {
        int endoff = s.length();
        char[] chars = s.toCharArray();
        for ( int offset = 0; offset < endoff; offset++ )
        {
            write( (byte) ( chars[offset] ) );
        }
    }

    @Override
    public void writeChars( String s )
        throws IOException
    {
        char[] chars = s.toCharArray();
        for ( char c : chars )
        {
            writeChar( c );
        }
    }

    @Override
    public void writeUTF( String s )
        throws IOException
    {
        // TODO Auto-generated method stub

    }

}
