# TiDB CDC SDK Extraction Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extract the embedded TiDB CDC client into a standalone Java SDK and make the Flink TiDB CDC connector consume that SDK.

**Architecture:** Preserve the existing `org.tikv.cdc` API surface while replacing Flink-specific inputs with primitive SDK inputs. Publish a Java 8 Maven jar that depends on `tikv-client-java`; keep Flink offset translation in the connector adapter.

**Tech Stack:** Java 8, Maven, TiKV Java Client 3.3.4-SNAPSHOT, JUnit 4, Guava, SLF4J

---

### Task 1: Scaffold the standalone SDK

**Files:**
- Create: `pom.xml`
- Modify: `README.md`

1. Define `org.tikv:java-client-cdc:1.0.0-SNAPSHOT`, Java 8, runtime dependencies, JUnit, and compiler/surefire plugins.
2. Document installation, dependency coordinates, and a minimal CDC client example.
3. Run `mvn -DskipTests package`; expect compilation to initially expose remaining Flink imports.

### Task 2: Migrate and decouple CDC sources

**Files:**
- Create: `src/main/java/org/tikv/**`
- Modify: `src/main/java/org/tikv/cdc/kv/CDCClient.java`
- Modify: `src/main/java/org/tikv/cdc/kv/GRPCClient.java`
- Modify: `src/main/java/org/tikv/common/util/TableKeyRangeUtils.java`
- Modify: `src/main/java/org/tikv/cdc/exception/ServerException.java`

1. Copy the connector-owned `org.tikv` implementation into the SDK.
2. Replace `CDCEventOffset` with a primitive end timestamp and a `Long.MAX_VALUE` no-stop sentinel.
3. Replace Flink shaded utility imports with ordinary Guava and remove the Flink executor type check.
4. Assert with `rg 'org\\.apache\\.flink' src/main/java`; expect no matches.

### Task 3: Migrate SDK unit tests

**Files:**
- Create: `src/test/java/org/tikv/cdc/frontier/**`
- Create: `src/test/java/org/tikv/cdc/kv/{GRPCClientTest,MatcherTest,RegionWorkerTest}.java`
- Create: `src/test/java/org/tikv/cdc/model/RegionKeyRangeTest.java`

1. Move tests that do not depend on the Flink integration test base.
2. Replace any connector-shaded test utility imports with normal Guava.
3. Run `mvn test`; expect all SDK unit tests to pass.

### Task 4: Switch the connector to the SDK

**Files:**
- Modify: `flink-connector-tidb-cdc/pom.xml`
- Modify: `.../source/fetch/CDCEventSource.java`
- Delete: `flink-connector-tidb-cdc/src/main/java/org/tikv/**`

1. Add `org.tikv:java-client-cdc:1.0.0-SNAPSHOT` and remove the redundant direct TiKV client declaration.
2. Convert the split ending offset commit version to `long` when constructing `CDCClient`.
3. Delete connector-local SDK implementation sources.
4. Build the SDK into the local Maven repository, then compile/test the connector module.

### Task 5: Verify the split

1. Run SDK tests and package verification.
2. Verify connector main sources contain no embedded `org/tikv` files.
3. Compile the connector against the locally installed SDK.
4. Review both Git worktrees and report exact changed files and any environment limitations.

