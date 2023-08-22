package org.apache.orc.impl;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.util.TreeMap;

public class LatticeRowBatch extends VectorizedRowBatch {

    public LongColumnVector intBuffer;
    public BytesColumnVector textBuffer;

    // this is a pretty big hack to track the portion of each buffer that needs to be
    // captured for each row. It's a huge hack because it's not clear how to fill this
    // for the read case. So it's fine if you're writing to the batch.. really it's just
    // a way for the batch writer to convey this information to the ORC writer... but
    // gah we need to deal with the fact that if we're taking chunks from the buffers
    // then we need to rewrite the offsets in the data or "re-home" the offsets
    // eff. ok for now I'm just going to say that we take a batch and that becomes your
    // stripe?!? at least for the next two days.
    // private TreeMap<Integer, Integer[]> bufferPositionsByOffset = new TreeMap<>();

    public LatticeRowBatch(int numCols, int bufferSize) {
        super(numCols);
        intBuffer = new LongColumnVector();
        textBuffer = new BytesColumnVector();
    }

    public LatticeRowBatch(int numCols, int size, int bufferSize) {
        super(numCols, size);
        intBuffer = new LongColumnVector();
        textBuffer = new BytesColumnVector();
    }
}
