package poc.kafka.log;

import org.apache.kafka.common.record.FileRecords;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        try (var records = FileRecords.open(new File("data/segment"))) {
            System.out.println(records.sizeInBytes());
        }
    }
}
