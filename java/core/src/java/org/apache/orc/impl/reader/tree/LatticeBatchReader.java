package org.apache.orc.impl.reader.tree;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.LatticeRowBatch;
import org.apache.orc.impl.OrcFilterContextImpl;
import org.apache.orc.impl.PositionProvider;
import org.apache.orc.impl.TreeReaderFactory;
import org.apache.orc.impl.reader.StripePlanner;

import java.io.IOException;
import java.util.List;

/**
 * Reads the primary stripe data using the delegate rootReader.
 * Loads the open buffer data into the buffers held by the LatticeRowBatch.
 */
public class LatticeBatchReader extends BatchReader {

    private final TreeReaderFactory.Context context;
    private final OrcFilterContextImpl filterContext;
    private final BatchReader dataReader;

    private final int[] bufferColIds;
    private final TypeReader[] bufferReaders;
    private final int[] bufferLengths;

    public LatticeBatchReader(BatchReader dataReader,
                              TypeReader[] bufferReaders,
                              int[] bufferColIds,
                              TreeReaderFactory.Context context) {
        super(dataReader.rootType);
        this.context = context;
        this.filterContext = new OrcFilterContextImpl(
                context.getSchemaEvolution().getReaderSchema(),
                context.getSchemaEvolution().isSchemaEvolutionCaseAware());

        this.dataReader = dataReader;
        this.bufferReaders = bufferReaders;
        this.bufferColIds = bufferColIds;
        this.bufferLengths = new int[bufferReaders.length];
    }

    public static LatticeBatchReader create(TypeDescription latticeDesc, TreeReaderFactory.Context context) throws IOException {
        final List<TypeDescription> children = latticeDesc.getChildren();
        final BatchReader rootReader = TreeReaderFactory.createRootReader(children.get(0), context);

        final TypeReader[] bufferReaders = new TypeReader[children.size() - 1];
        final int[] bufferColIds = new int[children.size() - 1];

        int i = 0;
        for (TypeDescription child : children.subList(1, children.size())) {
            bufferReaders[i] = TreeReaderFactory.createTreeReader(child, context);
            bufferColIds[i] = child.getId();
            i++;
        }

        return new LatticeBatchReader(rootReader, bufferReaders, bufferColIds, context);
    }

    @Override
    public void startStripe(StripePlanner planner, TypeReader.ReadPhase readPhase) throws IOException {
        dataReader.startStripe(planner, readPhase);

        for (int i = 0; i < bufferColIds.length; i++) {
            bufferLengths[i] = (int) planner.getColumnLength(bufferColIds[i]);
        }

        for (TypeReader bufferReader : bufferReaders) {
            bufferReader.startStripe(planner, readPhase);
        }
    }

    @Override
    public void nextBatch(VectorizedRowBatch batch, int batchSize, TypeReader.ReadPhase readPhase) throws IOException {
        LatticeRowBatch latticeBatch = (LatticeRowBatch) batch;

        dataReader.nextBatch(latticeBatch, batchSize, readPhase);

        for (int i = 0; i < bufferReaders.length; i++) {
            final ColumnVector buffer = latticeBatch.buffers[i];
            buffer.reset();
            buffer.ensureSize(bufferLengths[i], false);
            bufferReaders[i].nextVector(
                    buffer,
                    null,
                    bufferLengths[i],
                    batch,
                    readPhase);
        }
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
