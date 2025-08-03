package com.extractor.flink.jobs.landing;

import com.extractor.flink.functions.KafkaProperties;
import com.extractor.flink.functions.MySQLDebeziumColumns;
import com.extractor.flink.utils.TopicNameBuilder;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ShippingServicesLandingJob {

	static String sourceTopic = "mysql-changes.shipping_db.shipping_services";
	public static String sinkTopic = TopicNameBuilder.build("shipping.shipping_services.landing");
	static String groupId = System.getenv("GROUP_ID");

	public static void main(String[] args) throws Exception {

		// Set up execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		String sourceProperties = KafkaProperties.build(sourceTopic, groupId);
		String createTableDebeziumColumns = MySQLDebeziumColumns.createTableFormat();

		String createSourceTableSql = String.format("""
				CREATE TABLE shipping_services (
				    payload STRING
				) %s
				""", sourceProperties);
		tableEnv.executeSql(createSourceTableSql);

		String sinkProperties = KafkaProperties.build(sinkTopic, groupId);

		String createSinkTableSql = String.format("""
				CREATE TABLE shipping_services_processed (
				    service_id INTEGER,
				    carrier_id INTEGER,
				    service_name STRING,
				    estimated_days INTEGER,
				    cost_estimate STRING,
				    %s
				) %s
				""", createTableDebeziumColumns, sinkProperties);
		tableEnv.executeSql(createSinkTableSql);

		String insertTableDebeziumColumns = MySQLDebeziumColumns.insertTableFormat();
		String insertSinkSql = String.format("""
				INSERT INTO shipping_services_processed
				SELECT
				    CAST(JSON_VALUE(payload, '$.after.service_id') AS INTEGER) AS service_id,
				    CAST(JSON_VALUE(payload, '$.after.carrier_id') AS INTEGER) AS carrier_id,
					CAST(JSON_VALUE(payload, '$.after.service_name') AS STRING) AS service_name,
				    CAST(JSON_VALUE(payload, '$.after.estimated_days') AS INTEGER) AS estimated_days,
				    CAST(JSON_VALUE(payload, '$.after.cost_estimate') AS STRING) AS cost_estimate,
				    %s
				FROM shipping_services
				""", insertTableDebeziumColumns);
		tableEnv.executeSql(insertSinkSql);
	}
}