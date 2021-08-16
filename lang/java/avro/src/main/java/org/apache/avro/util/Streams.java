package org.apache.avro.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class Streams {

  private Streams() {
  }

  /**
   * see copy(final InputStream is, final OutputStream os, final int buffSize) for detail.
   *
   * @param is
   * @param os
   * @return
   * @throws IOException
   */
  public static long copy(final InputStream is, final OutputStream os) throws IOException {
    return copy(is, os, 8192);
  }

  /**
   * Equivalent to guava ByteStreams.copy, with one special behavior: if is has no bytes immediately available for read,
   * the os is flushed prior to the next read that will probably block.
   *
   * I believe this behavior will yield better performance in most scenarios. This method also makes use of:
   * Arrays.getBytesTmp. THis method should not be invoked from any context making use of Arrays.getBytesTmp.
   *
   * @param is
   * @param os
   * @param buffSize
   * @throws IOException
   */
  public static long copy(final InputStream is, final OutputStream os, final int buffSize) throws IOException {
    if (buffSize < 2) {
      int val;
      long count = 0;
      while ((val = is.read()) >= 0) {
        os.write(val);
        count++;
      }
      return count;
    }
    long total = 0;
    byte[] buffer = Arrays.getBytesTmp(buffSize);
    boolean done = false;
    long bytesCopiedSinceLastFlush = 0;
    while (true) {
      // non-blocking(if input is implemented correctly) read+write as long as data is available.
      while (is.available() > 0) {
        int nrRead = is.read(buffer, 0, buffSize);
        if (nrRead < 0) {
          done = true;
          break;
        } else {
          os.write(buffer, 0, nrRead);
          total += nrRead;
          bytesCopiedSinceLastFlush += nrRead;
        }
      }
      // there seems to be nothing available to read anymore,
      // lets flush the os instead of blocking for another read.
      if (bytesCopiedSinceLastFlush > 0) {
        os.flush();
        bytesCopiedSinceLastFlush = 0;
      }
      if (done) {
        break;
      }
      // most likely a blocking read.
      int nrRead = is.read(buffer, 0, buffSize);
      if (nrRead < 0) {
        break;
      } else {
        os.write(buffer, 0, nrRead);
        total += nrRead;
        bytesCopiedSinceLastFlush += nrRead;
      }
    }
    return total;
  }

}
