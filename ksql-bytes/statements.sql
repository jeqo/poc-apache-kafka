CREATE STREAM s1
(
  id STRING KEY,
  data BYTES
)
WITH (
  kafka_topic = 't1',
  partitions = 6,
  replicas = 1,
  value_format = 'json'
);

INSERT INTO s1 VALUES ('abc1', []);

INSERT INTO s1 VALUES ('abc2', [01]);

INSERT INTO s1 VALUES ('abc3', [1A0A1A]);

SELECT * FROM s1 EMIT CHANGES;
