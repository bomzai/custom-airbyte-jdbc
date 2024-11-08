/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.as400;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.airbyte.cdk.db.SqlDatabase;
import io.airbyte.cdk.db.jdbc.JdbcDatabase;
import io.airbyte.cdk.db.jdbc.JdbcUtils;
import io.airbyte.cdk.db.jdbc.StreamingJdbcDatabase;
import io.airbyte.cdk.db.jdbc.streaming.AdaptiveStreamingQueryConfig;
import io.airbyte.cdk.integrations.base.IntegrationRunner;
import io.airbyte.cdk.integrations.base.Source;
import io.airbyte.cdk.integrations.source.jdbc.AbstractJdbcSource;
import io.airbyte.cdk.integrations.source.jdbc.JdbcDataSourceUtils;
import io.airbyte.cdk.integrations.source.jdbc.dto.JdbcPrivilegeDto;
import io.airbyte.cdk.integrations.source.relationaldb.TableInfo;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.CommonField;
import io.airbyte.protocol.models.JsonSchemaType;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class As400Source extends AbstractJdbcSource<JDBCType> implements Source {

  private static final Logger LOGGER = LoggerFactory.getLogger(As400Source.class);

  // TODO insert your driver name. Ex: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  static final String DRIVER_CLASS = "com.ibm.as400.access.AS400JDBCDriver";

  public As400Source() {
    // TODO: if the JDBC driver does not support custom fetch size, use NoOpStreamingQueryConfig
    // instead of AdaptiveStreamingQueryConfig.
    super(DRIVER_CLASS, AdaptiveStreamingQueryConfig::new, JdbcUtils.getDefaultSourceOperations());
  }

  // TODO The config is based on spec.json, update according to your DB
  @Override
  public JsonNode toDatabaseConfig(final JsonNode config) {
    LOGGER.info("toDatabaseConfig function is starting ..");
    final String encodedLibrary = URLEncoder.encode(config.get("library").asText(), StandardCharsets.UTF_8);
    final StringBuilder jdbcUrl = new StringBuilder(String.format("jdbc:as400://%s;naming=system;libraries=%s",
            config.get(JdbcUtils.HOST_KEY).asText(),
            encodedLibrary));

    // add any specific connection properties as needed
    if (config.get(JdbcUtils.JDBC_URL_PARAMS_KEY) != null && !config.get(JdbcUtils.JDBC_URL_PARAMS_KEY).asText().isEmpty()) {
      jdbcUrl.append(JdbcUtils.AMPERSAND).append(config.get(JdbcUtils.JDBC_URL_PARAMS_KEY).asText());
    }

    final ImmutableMap.Builder<Object, Object> configBuilder = ImmutableMap.builder()
            .put(JdbcUtils.USERNAME_KEY, config.get(JdbcUtils.USERNAME_KEY).asText())
            .put(JdbcUtils.JDBC_URL_KEY, jdbcUrl.toString());


    if (config.has(JdbcUtils.PASSWORD_KEY)) {
      configBuilder.put(JdbcUtils.PASSWORD_KEY, config.get(JdbcUtils.PASSWORD_KEY).asText());
    }

    LOGGER.info("toDatabaseConfig function is configured ..");

    return Jsons.jsonNode(configBuilder.build());
  }

  @Override
  public JdbcDatabase createDatabase(final JsonNode sourceConfig, String delimiter) throws SQLException {
    LOGGER.info("@Override createDatabse function is starting ..");
    final JsonNode jdbcConfig = toDatabaseConfig(sourceConfig);
    Map<String, String> connectionProperties = JdbcDataSourceUtils.getConnectionProperties(sourceConfig, delimiter);

    // Set up connection properties
    String username = jdbcConfig.has(JdbcUtils.USERNAME_KEY) ? jdbcConfig.get(JdbcUtils.USERNAME_KEY).asText() : null;
    String password = jdbcConfig.has(JdbcUtils.PASSWORD_KEY) ? jdbcConfig.get(JdbcUtils.PASSWORD_KEY).asText() : null;
    String jdbcUrl = jdbcConfig.get(JdbcUtils.JDBC_URL_KEY).asText();

    // Create HikariConfig and set properties
    HikariConfig config = new HikariConfig();

    LOGGER.info("@Override createDatabse function : DRIVER_CLASS " + DRIVER_CLASS);
    LOGGER.info("@Override createDatabse function : jdbcUrl " + jdbcUrl);
    LOGGER.info("@Override createDatabse function : username " + username);
    try {
      Class.forName(DRIVER_CLASS);
    } catch (ClassNotFoundException e){
      LOGGER.info("@Override createDatabse function : AS400 Driver class not found ");
    }

    config.setDriverClassName(DRIVER_CLASS);  // Use your AS400 driver class
    config.setJdbcUrl(jdbcUrl);
    config.setUsername(username);
    config.setPassword(password);

    // Set connection timeout
    config.setConnectionTimeout(Duration.ofSeconds(30).toMillis());  // Timeout in milliseconds

    // Add other properties from connectionProperties
    for (Map.Entry<String, String> entry : connectionProperties.entrySet()) {
      config.addDataSourceProperty(entry.getKey(), entry.getValue());
    }
    LOGGER.info("@Override createDatabse function : creating HikariDataSource ..");
    // Create HikariDataSource
    HikariDataSource dataSource = new HikariDataSource(config);

    // Record the data source so that it can be closed.
    dataSources.add(dataSource);
    LOGGER.info("@Override createDatabse function : creating StreamingJdbcDatabase: databse ..");
    final JdbcDatabase database = new StreamingJdbcDatabase(
            dataSource,
            sourceOperations,
            streamingQueryConfigProvider);

    quoteString = (quoteString == null ? database.getMetaData().getIdentifierQuoteString() : quoteString);
    database.setSourceConfig(sourceConfig);
    database.setDatabaseConfig(jdbcConfig);

    LOGGER.info("@Override createDatabse function : set config StreamingJdbcDatabase: databse ..");
    return database;
  }


  private List<String> extractCursorFields(List<JsonNode> fields) {
    return fields.stream()
            .filter(field -> this.isCursorType(this.sourceOperations.getDatabaseFieldType(field)))
            .map(field -> field.get("columnName").asText())
            .collect(Collectors.toList());
  }

  private String getCatalog(SqlDatabase database) {
    return database.getSourceConfig().has("database") ? database.getSourceConfig().get("database").asText() : null;
  }

  private JsonNode getColumnMetadata(ResultSet resultSet) throws SQLException {
    ImmutableMap.Builder<Object, Object> fieldMap = ImmutableMap.builder()
            .put("schemaName", resultSet.getObject("TABLE_SCHEM") != null ? resultSet.getString("TABLE_SCHEM") : resultSet.getObject("TABLE_CAT"))
            .put("tableName", resultSet.getString("TABLE_NAME"))
            .put("columnName", resultSet.getString("COLUMN_NAME"))
            .put("columnType", resultSet.getString("DATA_TYPE"))
            .put("columnTypeName", resultSet.getString("TYPE_NAME"))
            .put("columnSize", resultSet.getInt("COLUMN_SIZE"))
            .put("isNullable", resultSet.getString("IS_NULLABLE"));

    if (resultSet.getString("DECIMAL_DIGITS") != null) {
      fieldMap.put("decimalDigits", resultSet.getString("DECIMAL_DIGITS"));
    }

    return Jsons.jsonNode(fieldMap.build());
  }

  @Override
  protected List<TableInfo<CommonField<JDBCType>>> discoverInternal(JdbcDatabase database, String schema) throws Exception {
    LOGGER.info("Starting schema discovery in discoverInternal with schema: {}", schema);

    Set<String> internalSchemas = new HashSet<>(this.getExcludedInternalNameSpaces());
    LOGGER.info("Internal schemas to exclude: {}", internalSchemas);

    Set<JdbcPrivilegeDto> tablesWithSelectGrantPrivilege = this.getPrivilegesTableForCurrentUser(database, schema);
    LOGGER.info("Tables with SELECT grant privilege: {}", tablesWithSelectGrantPrivilege);

    List<TableInfo<CommonField<JDBCType>>> tableInfoList = database.bufferedResultSetQuery(
                    (connection) -> {
                      LOGGER.info("Executing metadata query for schema discovery...");
                      return connection.getMetaData().getColumns(
                              this.getCatalog(database),   // Catalog might be null for AS400
                              "BIBFM",                      // Pass schema or null as needed
                              null,                        // Table name pattern (null means all tables)
                              null                         // Column name pattern (null means all columns)
                      );
                    },
                    this::getColumnMetadata
            ).stream()
            .peek(result -> LOGGER.debug("Column metadata retrieved: {}", result))
            .filter(this.excludeNotAccessibleTables(internalSchemas, tablesWithSelectGrantPrivilege))
            .peek(result -> LOGGER.debug("Filtered result after access check: {}", result))
            .collect(Collectors.groupingBy(t -> {
              String schemaName = t.get("schemaName").asText();
              String tableName = t.get("tableName").asText();
              LOGGER.info("Grouping by schema: {}, table: {}", schemaName, tableName);
              return ImmutablePair.of(schemaName, tableName);
            }))
            .values()
            .stream()
            .map(fields -> {
              String schemaName = fields.get(0).get("schemaName").asText();
              String tableName = fields.get(0).get("tableName").asText();
              LOGGER.info("Processing table: {} in schema: {}", tableName, schemaName);

              List<CommonField<JDBCType>> columns = fields.stream().map(f -> {
                String columnName = f.get("columnName").asText();
                String columnTypeName = f.get("columnTypeName").asText();
                int columnSize = f.get("columnSize").asInt();
                boolean isNullable = f.get("isNullable").asBoolean();

                JDBCType datatype = this.sourceOperations.getDatabaseFieldType(f);
                JsonSchemaType jsonType = this.getAirbyteType(datatype);

                LOGGER.debug("Table: {} Column: {} (Type: {}[{}], Nullable: {}) -> {}", tableName, columnName, columnTypeName, columnSize, isNullable, jsonType);

                return new CommonField<>(columnName, datatype);
              }).collect(Collectors.toList());

              List<String> cursorFields = this.extractCursorFields(fields);
              LOGGER.info("Extracted cursor fields for table {}: {}", tableName, cursorFields);

              return TableInfo.<CommonField<JDBCType>>builder()
                      .nameSpace(schemaName)
                      .name(tableName)
                      .fields(columns)
                      .cursorFields(cursorFields)
                      .build();
            })
            .collect(Collectors.toList());

    LOGGER.info("Completed schema discovery with {} tables found.", tableInfoList.size());

    return tableInfoList;
  }


  @Override
  public Set<String> getExcludedInternalNameSpaces() {
    // TODO Add tables to exclude, Ex "INFORMATION_SCHEMA", "sys", "spt_fallback_db", etc
    return Set.of("");
  }


  public static void main(final String[] args) throws Exception {
    final Source source = new As400Source();
    LOGGER.info("starting source: {}", As400Source.class);
    new IntegrationRunner(source).run(args);
    LOGGER.info("completed source: {}", As400Source.class);
  }

}
