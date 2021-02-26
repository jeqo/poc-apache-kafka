CREATE STREAM orders (ORDERID INT KEY, ORDERUNITS double, NAME string) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM S1 AS SELECT ORDERID, ORDERUNITS, CASE WHEN orderunits < 2.0 THEN 'small' WHEN orderunits < 4.0 THEN 'medium' ELSE 'large' END AS case_result, custom_split(name) AS words FROM orders EMIT CHANGES;
