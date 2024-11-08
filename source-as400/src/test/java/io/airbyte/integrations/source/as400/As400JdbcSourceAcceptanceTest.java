/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.as400;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.cdk.integrations.source.jdbc.AbstractJdbcSource;
import io.airbyte.cdk.integrations.source.jdbc.test.JdbcSourceAcceptanceTest;
import java.sql.JDBCType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.jupiter.api.Disabled;
import io.airbyte.cdk.testutils.TestDatabase;

@Disabled
class As400JdbcSourceAcceptanceTest extends JdbcSourceAcceptanceTest<As400Source,As400JavaJdbcTestDatabase> {

  private static final Logger LOGGER = LoggerFactory.getLogger(As400JdbcSourceAcceptanceTest.class);

  // TODO declare a test container for DB. EX: org.testcontainers.containers.OracleContainer

//  @BeforeAll
//  static void init() {
//    // Oracle returns uppercase values
//    // TODO init test container. Ex: "new OracleContainer("epiclabs/docker-oracle-xe-11g")"
//    // TODO start container. Ex: "container.start();"
//  }
//
//  @BeforeEach
//  public void setup() throws Exception {
//    // TODO init config. Ex: "config = Jsons.jsonNode(ImmutableMap.builder().put("host",
//    // host).put("port", port)....build());
//    super.setup();
//  }
//
//  @AfterEach
//  public void tearDown() {
//    // TODO clean used resources
//  }
//
//  @Override
//  public AbstractJdbcSource<JDBCType> getSource() {
//    return new As400Source();
//  }
//
  @Override
  public boolean supportsSchemas() {
    // TODO check if your db supports it and update method accordingly
    return false;
  }

  @Override
  protected As400JavaJdbcTestDatabase createTestDatabase() {
    return null;
  }

  @Override
  protected As400Source source() {
    // TODO: (optional) call `setFeatureFlags` before returning the source to mock setting env vars.
    return new As400Source();
  }

  @Override
  protected JsonNode config() {
    // TODO: (optional) call more builder methods.
    return null;
  }
//
//  @Override
//  public String getDriverClass() {
//    return As400Source.DRIVER_CLASS;
//  }
//
//  @Override
//  public AbstractJdbcSource<JDBCType> getJdbcSource() {
//    // TODO
//    return null;
//  }
//
//  @AfterAll
//  static void cleanUp() {
//    // TODO close the container. Ex: "container.close();"
//  }

}
