package com.github.noctarius.waljdbc.io;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;

public class DataByteArrayInputBuffer
    extends ByteArrayInputStream
    implements DataInput
{

    public DataByteArrayInputBuffer( byte[] buf, int offset, int length )
    {
        super( buf, offset, length );
    }

    public DataByteArrayInputBuffer( byte[] buf )
    {
        super( buf );
    }

    @Override
    public void readFully( byte[] b )
        throws IOException
    {
        readFully( b, 0, b.length );
    }

    @Override
    public void readFully( byte[] b, int off, int len )
        throws IOException
    {
        int length = Math.min( len, count - pos );
        System.arraycopy( buf, pos, b, off, length );
    }

    @Override
    public int skipBytes( int n )
        throws IOException
    {
        return (int) skip( n );
    }

    @Override
    public boolean readBoolean()
        throws IOException
    {
        int v = read();
        if ( v < 0 )
        {
            throw new EOFException();
        }
        return ( v != 0 );
    }

    @Override
    public byte readByte()
        throws IOException
    {
        int v = read();
        if ( v < 0 )
        {
            throw new EOFException();
        }
        return (byte) v;
    }

    @Override
    public int readUnsignedByte()
        throws IOException
    {
        int v = read();
        if ( v < 0 )
        {
            throw new EOFException();
        }
        return v;
    }

    @Override
    public short readShort()
        throws IOException
    {
        short v = (short) ( ( buf[pos + 1] & 0xFF ) + ( buf[pos] << 8 ) );
        pos += 2;
        return v;
    }

    @Override
    public int readUnsignedShort()
        throws IOException
    {
        return readShort() & 0xFFFF;
    }

    @Override
    public char readChar()
        throws IOException
    {
        char v = (char) ( ( buf[pos + 1] & 0xFF ) + ( buf[pos] << 8 ) );
        pos += 2;
        return v;
    }

    @Override
    public int readInt()
        throws IOException
    {
        return ( ( buf[pos + 3] & 0xFF ) ) + ( ( buf[pos + 2] & 0xFF ) << 8 ) + ( ( buf[pos + 1] & 0xFF ) << 16 )
            + ( ( buf[pos] ) << 24 );
    }

    @Override
    public long readLong()
        throws IOException
    {
        return ( ( (long) buf[pos + 7] & 0xFFL ) ) + ( ( (long) buf[pos + 6] & 0xFFL ) << 8 )
            + ( ( (long) buf[pos + 5] & 0xFFL ) << 16 ) + ( ( (long) buf[pos + 4] & 0xFFL ) << 24 )
            + ( ( (long) buf[pos + 3] & 0xFFL ) << 32 ) + ( ( (long) buf[pos + 2] & 0xFFL ) << 40 )
            + ( ( (long) buf[pos + 1] & 0xFFL ) << 48 ) + ( ( (long) buf[pos] ) << 56 );
    }

    @Override
    public float readFloat()
        throws IOException
    {
        return Float.intBitsToFloat( readInt() );
    }

    @Override
    public double readDouble()
        throws IOException
    {
        return Double.longBitsToDouble( readLong() );
    }

    @Override
    public String readLine()
        throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF()
        throws IOException
    {
        short length = readShort();
        byte[] stringData = new byte[length];
        readFully( stringData );
        return new String( stringData, "UTF-8" );
    }

    public int pos()
    {
        return pos;
    }

    public void pos( int pos )
    {
        this.pos = pos;
    }

}
