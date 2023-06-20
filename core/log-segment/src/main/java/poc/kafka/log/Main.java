package poc.kafka.log;

import kafka.log.OffsetIndex;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.RecordBatch;

import java.io.File;
import java.io.IOException;

/**
 * Experimenting with log segments and index files to obtain metadata.
 */
public class Main {
    public static void main(String[] args) throws IOException {
        String prefix = "data/0PVBFBxoR2KSP8imYNgisQ";
        File segmentFile = new File(prefix + "/0PVBFBxoR2KSP8imYNgisQ.log");
        File offsetIndexFile = new File(prefix + "/0PVBFBxoR2KSP8imYNgisQ_offset.index");

        try (var records = FileRecords.open(segmentFile)) {
            System.out.println(records.sizeInBytes());
            RecordBatch first = records.firstBatch();
            System.out.println("Compression at start: " + first.compressionType());
            {
                // Alternative 0: only use the first batch
                System.out.println("1: Compression at end: " + first.compressionType() + "] batches: ");
            }

            {
                // Alternative 1: naive iteration over batches
                CompressionType lastCompressionType = null;
                long lastBaseOffset = -1;
                long lastLastOffset = -1;
                int i = 0;
                for (var r : records.batches()) {
                    lastCompressionType = r.compressionType();
                    lastBaseOffset = r.baseOffset();
                    lastLastOffset = r.lastOffset();
                    i++;
                }
                System.out.println("1: Compression at end: " + lastCompressionType
                        + " from batch : [" + lastBaseOffset + ":" + lastLastOffset + "] batches: " + i);
            }

            {
                // Alternative 2: use offset index
                try (var offsetIndex = new OffsetIndex(offsetIndexFile, first.baseOffset(), -1, false)) {
                    System.out.println("2: Last offset from Index: " + offsetIndex.lastOffset());

                    var lastPos = offsetIndex.lookup(offsetIndex.lastOffset());

                    System.out.println("2: Last position: " + lastPos.position());

                    // obtain the very last
                    CompressionType lastCompressionType = null;
                    long lastBaseOffset = -1;
                    long lastLastOffset = -1;
                    int i = 0;
                    for (var r : records.batchesFrom(lastPos.position())) {
                        lastCompressionType = r.compressionType();
                        lastBaseOffset = r.baseOffset();
                        lastLastOffset = r.lastOffset();
                        i++;
                    }
                    System.out.println("2.1: Compression at end: " + lastCompressionType
                            + " from batch : [" + lastBaseOffset + ":" + lastLastOffset + "] batches: " + i);

                    // only from the first last batch (expecting only one, but could be more)
                    var iter = records.batchesFrom(lastPos.position()).iterator();
                    if (iter.hasNext()) {
                        var last = iter.next();
                        System.out.println("2.2: Compression at end: " + last.compressionType());
                    }
                }
            }
        }
    }
}
