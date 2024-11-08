/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.as400;

import io.airbyte.cdk.db.factory.DatabaseDriver;
import io.airbyte.cdk.testutils.TestDatabase;
import java.util.stream.Stream;
import org.jooq.SQLDialect;
import org.testcontainers.containers.JdbcDatabaseContainer;

public class As400JavaJdbcTestDatabase
    extends TestDatabase<JdbcDatabaseContainer<?>, As400JavaJdbcTestDatabase, As400JavaJdbcTestDatabase.as400JavaJdbcConfigBuilder> {

  public As400JavaJdbcTestDatabase(JdbcDatabaseContainer<?> container) {
    // TODO: (optional) consider also implementing a ContainerFactory to share testcontainer instances.
    // Effective use requires parallelizing the tests using JUnit instead of gradle.
    // This is best achieved by adding a `gradle.properties` file containing
    // `testExecutionConcurrency=-1`.
    super(container);
  }

  @Override
  protected Stream<Stream<String>> inContainerBootstrapCmd() {
    // TODO: return a stream of streams of command args to be passed to `execInContainer` calls to set
    // up the test state.
    // This usually involves the execution of CREATE DATABASE and CREATE USER statements as root.
    return Stream.empty();
  }

  @Override
  protected Stream<String> inContainerUndoBootstrapCmd() {
    // TODO: (optional) return a stream of command args to be passed to a `execInContainer` call to
    // clean up the test state.
    return Stream.empty();
  }

  @Override
  public DatabaseDriver getDatabaseDriver() {
    // TODO: return a suitable value.

    return DatabaseDriver.POSTGRESQL;
  }

  @Override
  public SQLDialect getSqlDialect() {
    // TODO: return a suitable value.
    return SQLDialect.DEFAULT;
  }

  @Override
  public as400JavaJdbcConfigBuilder configBuilder() {
    // TODO: flesh out the ConfigBuilder subclass and return a new instance of it here.
    return new as400JavaJdbcConfigBuilder(this);
  }

  public static class as400JavaJdbcConfigBuilder extends TestDatabase.ConfigBuilder<As400JavaJdbcTestDatabase, as400JavaJdbcConfigBuilder> {

    public as400JavaJdbcConfigBuilder(As400JavaJdbcTestDatabase testDatabase) {
      super(testDatabase);
    }

  }

}
