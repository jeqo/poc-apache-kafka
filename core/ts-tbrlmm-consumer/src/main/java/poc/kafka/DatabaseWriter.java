package poc.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;

import java.io.Closeable;
import java.io.IOException;
import java.sql.*;
import java.util.List;

public class DatabaseWriter implements Closeable {

    final Connection conn;
    final PreparedStatement ps;

    public DatabaseWriter(String dbName) throws SQLException {
        var jdbcUrl = "jdbc:sqlite:%s.db".formatted(dbName);
        this.conn = DriverManager.getConnection(jdbcUrl);

        var statement = conn.createStatement();

        statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS rlmm_events (
                    _partition INTEGER NOT NULL,
                    _offset LONG NOT NULL,
                    
                    broker_id INTEGER NOT NULL,
                    ts LONG NOT NULL,
                    
                    topic_id TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    partition INTEGER NOT NULL,
                    
                    event_type TEXT NOT NULL,
                    state TEXT NOT NULL,
                    
                    segment_id TEXT NOT NULL,
                    
                    start_offset LONG,
                    end_offset LONG,
                    max_timestamp_ms LONG,
                    segment_size_in_bytes INT
                )
                """);

        ps = conn.prepareStatement("""
                INSERT INTO rlmm_events VALUES (
                    ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?,
                    ?, ?, ?, ?
                )
                """);
    }

    public void addRecords(List<ConsumerRecord<byte[], RemoteLogMetadata>> records) throws SQLException {
        for (final var r : records) {
            ps.setInt(1, r.partition());
            ps.setLong(2, r.offset());

            ps.setInt(3, r.value().brokerId());
            ps.setLong(4, r.value().eventTimestampMs());

            ps.setString(5, r.value().topicIdPartition().topicId().toString());
            ps.setString(6, r.value().topicIdPartition().topic());
            ps.setInt(7, r.value().topicIdPartition().partition());

            switch (r.value()) {
                case RemoteLogSegmentMetadata meta -> {
                    ps.setString(8, "RemoteLogSegmentMetadata");
                    ps.setString(9, meta.state().name());

                    ps.setString(10, meta.remoteLogSegmentId().id().toString());

                    ps.setLong(11, meta.startOffset());
                    ps.setLong(12, meta.endOffset());
                    ps.setLong(13, meta.maxTimestampMs());
                    ps.setInt(14, meta.segmentSizeInBytes());
                }
                case RemoteLogSegmentMetadataUpdate meta -> {
                    ps.setString(8, "RemoteLogSegmentMetadataUpdate");
                    ps.setString(9, meta.state().name());

                    ps.setString(10, meta.remoteLogSegmentId().id().toString());

                    ps.setNull(11, Types.BIGINT);
                    ps.setNull(12, Types.BIGINT);
                    ps.setNull(13, Types.BIGINT);
                    ps.setNull(14, Types.INTEGER);
                }
                default -> throw new IllegalStateException("Unexpected value: " + r.value());
            }

            ps.addBatch();
        }
    }


    @Override
    public void close() throws IOException {
        if (conn != null) {
            try {
                ps.executeBatch();
                conn.close();
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }
    }
}
