package org.example;

/**
 * Created by vishnu on 31/12/14.
 */
public class util {
    public static long toLong(byte[] data) {
        if (data == null || data.length != 8) return 0x0;
        // ----------
        return (long)(
                // (Below) convert to longs before shift because digits
                //         are lost with ints beyond the 32-bit limit
                (long)(0xff & data[0]) << 56  |
                        (long)(0xff & data[1]) << 48  |
                        (long)(0xff & data[2]) << 40  |
                        (long)(0xff & data[3]) << 32  |
                        (long)(0xff & data[4]) << 24  |
                        (long)(0xff & data[5]) << 16  |
                        (long)(0xff & data[6]) << 8   |
                        (long)(0xff & data[7]) << 0
        );
    }
    public static byte[] toBytes(long data) {
        return new byte[] {
                (byte)((data >> 56) & 0xff),
                (byte)((data >> 48) & 0xff),
                (byte)((data >> 40) & 0xff),
                (byte)((data >> 32) & 0xff),
                (byte)((data >> 24) & 0xff),
                (byte)((data >> 16) & 0xff),
                (byte)((data >> 8) & 0xff),
                (byte)((data >> 0) & 0xff),
        };
    }
}
