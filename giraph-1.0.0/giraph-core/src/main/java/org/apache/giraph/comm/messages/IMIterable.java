package org.apache.giraph.comm.messages;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.RepresentativeByteArrayIterable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
   * Special iterable that recycles the message
   */
  public class IMIterable<I extends WritableComparable, M extends Writable> extends 
  	RepresentativeByteArrayIterable<IMWritable<I,M>> {
    /**
     * Constructor
     *
     * @param buf Buffer
     * @param off Offset to start in the buffer
     * @param length Length of the buffer
     */
    public IMIterable(
        byte[] buf, int off, int length, ImmutableClassesGiraphConfiguration config) {
      super(config, buf, off, length);
    }

    @Override
    protected IMWritable<I,M> createWritable() {
      return new IMWritable<I,M>(configuration);
    }
  }