package org.apache.orc.impl.reader.tree;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.LatticeRowBatch;
import org.apache.orc.impl.OrcFilterContextImpl;
import org.apache.orc.impl.PositionProvider;
import org.apache.orc.impl.TreeReaderFactory;
import org.apache.orc.impl.reader.StripePlanner;

import java.io.IOException;

/**
 * Reads the primary stripe data using the delegate rootReader.
 * Loads the open buffer data into the buffers held by the LatticeRowBatch.
 */
public class LatticeBatchReader extends BatchReader {

    private final TreeReaderFactory.Context context;
    private final OrcFilterContextImpl filterContext;
    private final BatchReader dataReader;
    private final TypeReader[] bufferReaders;

    // todo: this is just a dummy, we need to determine the size based on something? or re-read as needed?
    //       i don;t know right now.
    private final static int bufferBatchSize = 256;

    public LatticeBatchReader(BatchReader dataReader,
                              TypeReader[] bufferReaders,
                              TreeReaderFactory.Context context) {
        super(dataReader.rootType);
        this.context = context;
        this.filterContext = new OrcFilterContextImpl(
                context.getSchemaEvolution().getReaderSchema(),
                context.getSchemaEvolution().isSchemaEvolutionCaseAware());

        this.dataReader = dataReader;
        this.bufferReaders = bufferReaders;
    }

    @Override
    public void startStripe(StripePlanner planner, TypeReader.ReadPhase readPhase) throws IOException {
        dataReader.startStripe(planner, readPhase);

        for (TypeReader bufferReader : bufferReaders) {
            bufferReader.startStripe(planner, readPhase);
        }
    }

    @Override
    public void nextBatch(VectorizedRowBatch batch, int batchSize, TypeReader.ReadPhase readPhase) throws IOException {
        LatticeRowBatch latticeBatch = (LatticeRowBatch) batch;

        dataReader.nextBatch(latticeBatch, batchSize, readPhase);

        latticeBatch.intBuffer.reset();
        latticeBatch.intBuffer.ensureSize(1, false);
        bufferReaders[0].nextVector(
                latticeBatch.intBuffer, null, 1, batch, readPhase);

        latticeBatch.textBuffer.reset();
        latticeBatch.textBuffer.ensureSize(1, false);
        bufferReaders[1].nextVector(
                latticeBatch.textBuffer, null, 1, batch, readPhase);
    }

    @Override
    public void skipRows(long rows, TypeReader.ReadPhase readPhase) throws IOException {
        dataReader.skipRows(rows, readPhase);
    }

    @Override
    public void seek(PositionProvider[] index, TypeReader.ReadPhase readPhase) throws IOException {
        dataReader.seek(index, readPhase);
    }
}
