package poc.kafka.log;

import kafka.log.OffsetIndex;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.RecordBatch;

import java.io.File;
import java.io.IOException;

/**
 * Experimenting with log segments and index files to obtain metadata.
 */
public class Main {
    public static void main(String[] args) throws IOException {
        try (var records = FileRecords.open(new File("data/0PVBFBxoR2KSP8imYNgisQ/0PVBFBxoR2KSP8imYNgisQ.log"))) {
            System.out.println(records.sizeInBytes());
            RecordBatch first = records.firstBatch();
            System.out.println("Compression at start: " + first.compressionType());

            try (var offsetIndex = new OffsetIndex(new File("data/0PVBFBxoR2KSP8imYNgisQ/0PVBFBxoR2KSP8imYNgisQ_offset.index"), first.baseOffset(), -1, false)) {
                System.out.println("Offset index " + offsetIndex.lastOffset());

                var lastPos = offsetIndex.lookup(offsetIndex.lastOffset());

                System.out.println("Last position: " + lastPos.position());
                var iter = records.batchesFrom(lastPos.position()).iterator();
                if (iter.hasNext()) {
                    var last = iter.next();
                    System.out.println("Compression at end: " + last.compressionType());
                }
            }
        }
    }
}
