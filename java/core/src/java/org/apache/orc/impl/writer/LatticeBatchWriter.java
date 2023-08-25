package org.apache.orc.impl.writer;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.LatticeRowBatch;

import java.io.IOException;

/**
 * Writes a batch of data for the data described by the schema, along with
 * the per-type buffer storage.
 *
 * todo: I think I need to move knowledge of the pointer maps directly into
 *       a structure that I can use here; the StructWriters need to communicate
 *       back their needs to the writer, and I need to communicate down any offset
 *       adjustments that they need to make. This is because there may be many batches
 *       to one stripe. I assume that a batch may be split across stripes, and while I'm
 *       not concerned about that, the above mechanism should fix that as well.
 *
 */
public class LatticeBatchWriter implements TreeWriter {

    final TreeWriter[] childrenWriters;

    /**
     * Create the delegate and buffer writers and set the initial record position.
     */
    public LatticeBatchWriter(TypeDescription schema,
                              WriterEncryptionVariant encryption,
                              WriterContext context) throws IOException {
        childrenWriters = new TreeWriter[schema.getChildren().size()];
        for (int i = 0; i < childrenWriters.length; ++i) {
            childrenWriters[i] = Factory.create(schema.getChildren().get(i), encryption, context);
        }
    }

    @Override
    public void writeRootBatch(VectorizedRowBatch batch, int offset,
                               int length) throws IOException {
        LatticeRowBatch latticeBatch = (LatticeRowBatch) batch;

        //indexStatistics.increment(length);
        childrenWriters[0].writeRootBatch(batch, offset, length);

        for (int i = 0; i + 1 < childrenWriters.length; i++) {
            childrenWriters[i + 1].writeBatch(
                    latticeBatch.buffers[i],
                    0,
                    latticeBatch.bufferSizes[i]);
        }
    }

    @Override
    public void writeBatch(ColumnVector vector, int offset, int length) throws IOException {
        throw new NotImplementedException("LatticeBatchWriter can only be the root of a tree!");
    }

    @Override
    public void createRowIndexEntry() throws IOException {
        for (TreeWriter child : childrenWriters) {
            child.createRowIndexEntry();
        }
    }

    @Override
    public void writeStripe(int requiredIndexEntries) throws IOException {
        // super.writeStripe(requiredIndexEntries);
        for (TreeWriter child : childrenWriters) {
            child.writeStripe(requiredIndexEntries);
        }
    }

    @Override
    public void addStripeStatistics(StripeStatistics[] stats
    ) throws IOException {
        // super.addStripeStatistics(stats);
        for (TreeWriter child : childrenWriters) {
            child.addStripeStatistics(stats);
        }
    }

    @Override
    public long estimateMemory() {
        long result = 0;
        for (TreeWriter writer : childrenWriters) {
            result += writer.estimateMemory();
        }
        return result;
    }

    @Override
    public long getRawDataSize() {
        long result = 0;
        for (TreeWriter writer : childrenWriters) {
            result += writer.getRawDataSize();
        }
        return result;
    }

    @Override
    public void writeFileStatistics() throws IOException {
        // super.writeFileStatistics();
        for (TreeWriter child : childrenWriters) {
            child.writeFileStatistics();
        }
    }

    @Override
    public void flushStreams() throws IOException {
        // super.flushStreams();
        for (TreeWriter child : childrenWriters) {
            child.flushStreams();
        }
    }

    @Override
    public void getCurrentStatistics(ColumnStatistics[] output) {
        // super.getCurrentStatistics(output);
        for (TreeWriter child: childrenWriters) {
            child.getCurrentStatistics(output);
        }
    }

    @Override
    public void prepareStripe(int stripeId) {
        // super.prepareStripe(stripeId);
        for (TreeWriter child: childrenWriters) {
            child.prepareStripe(stripeId);
        }
    }
}
