/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.vector;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.vector.index.IvfFlatVectorGlobalIndexerFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Generates a real primary-key vector table written with the production {@code ivf-flat} indexer,
 * so paimon-rust can commit the on-disk directory as a cross-language read-back fixture.
 *
 * <p>Gated behind {@code -Dgen.pkvector.fixture=true} so it never runs in normal CI. Run with:
 *
 * <pre>
 * mvn -pl paimon-vector test -Dtest=PkVectorFixtureGenerator \
 *     -Dgen.pkvector.fixture=true -Drun.e2e.tests=true \
 *     -Dspotless.check.skip=true -Dcheckstyle.skip=true
 * </pre>
 *
 * <p>It writes rows whose {@code id} equals the intended global row id (single first batch, so
 * {@code first_row_id == 0} and the id-to-row-id mapping is the identity), then compacts to build
 * the primary-key ANN segment ({@code PrimaryKeyIndexSourceMeta}). The produced table directory is
 * copied to {@code paimon-vector/target/pkvector-fixture/<table>} for extraction.
 */
public class PkVectorFixtureGenerator extends TableTestBase {

    private static final String TABLE = "pk_vector_ivf_flat";

    // Fixed 2-D vectors; id == row position (identity mapping). L2 distance to query [0,0]
    // is the squared norm: row0=0, row1=1, row2=4, row3=9, row4=16.
    private static final float[][] VECTORS = {
        {0.0f, 0.0f}, {1.0f, 0.0f}, {2.0f, 0.0f}, {3.0f, 0.0f}, {4.0f, 0.0f}
    };

    // Richer 6-row demo dataset (id == row position). Chosen so a single committed
    // Java-written table drives every read-side scenario in paimon-rust's
    // pk_vector_search_demo by only varying the query / filter / read-options:
    //   query [10,0]  -> squared-L2 [116,4,125,9,136,1] -> top-3 ids [5,1,3]
    //                    (best-first != ascending id/position order)
    //   residual id>=3 over [10,0]                      -> top-3 ids [5,3,4]
    //   query [0,10]                                    -> top-3 ids [4,2,0]
    //   query [6,3]                                     -> top-3 ids [3,1,5]
    // All queries have strict distance gaps, so every top-k order is unique.
    private static final String DEMO_TABLE = "pk_vector_demo";
    private static final float[][] DEMO_VECTORS = {
        {0.0f, 4.0f}, // id 0
        {8.0f, 0.0f}, // id 1
        {0.0f, 5.0f}, // id 2
        {7.0f, 0.0f}, // id 3
        {0.0f, 6.0f}, // id 4
        {9.0f, 0.0f}, // id 5
    };

    @Test
    @EnabledIfSystemProperty(named = "gen.pkvector.fixture", matches = "true")
    void generatePkVectorIvfFlatFixture() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()))
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.MERGE_ENGINE.key(), "deduplicate")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .option(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding")
                        .option(
                                "fields.embedding.pk-vector.index.type",
                                IvfFlatVectorGlobalIndexerFactory.IDENTIFIER)
                        .option("fields.embedding.pk-vector.distance.metric", "l2")
                        .option(IvfFlatVectorGlobalIndexerFactory.IDENTIFIER + ".dimension", "2")
                        .option(IvfFlatVectorGlobalIndexerFactory.IDENTIFIER + ".metric", "l2")
                        .option(IvfFlatVectorGlobalIndexerFactory.IDENTIFIER + ".nlist", "1")
                        .build();
        catalog.createTable(identifier(TABLE), schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier(TABLE));

        InternalRow[] rows = new InternalRow[VECTORS.length];
        for (int i = 0; i < VECTORS.length; i++) {
            rows[i] = GenericRow.of(i, BinaryVector.fromPrimitiveArray(VECTORS[i]));
        }
        write(table, ioManager, rows);
        compact(table, org.apache.paimon.data.BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Sanity: the ANN segment must exist after compaction (index manifest non-empty).
        assertThat(table.latestSnapshot()).isPresent();
        assertThat(table.latestSnapshot().get().indexManifest()).isNotNull();

        // Copy the on-disk table directory out of the auto-cleaned temp warehouse.
        // Resolve the real path from the table itself (the filesystem catalog lays
        // tables out under <db>.db/<table>, so don't hand-build the path). The
        // location URI is like "traceable:/abs/path/..."; strip the scheme prefix
        // down to the first absolute-path slash.
        String tableLocation = table.location().toString();
        int slash = tableLocation.indexOf('/');
        java.nio.file.Path src = Paths.get(tableLocation.substring(slash));
        java.nio.file.Path dst =
                Paths.get(System.getProperty("user.dir"), "target", "pkvector-fixture", TABLE);
        if (Files.exists(dst)) {
            deleteRecursively(dst);
        }
        Files.createDirectories(dst.getParent());
        copyRecursively(src, dst);

        System.out.println("[PkVectorFixtureGenerator] fixture written to: " + dst);
        System.out.println(
                "[PkVectorFixtureGenerator] vectors (id==rowid): "
                        + java.util.Arrays.deepToString(VECTORS));
        System.out.println(
                "[PkVectorFixtureGenerator] query [0,0] L2 top-3 expected row_ids = [0,1,2],"
                        + " distances = [0.0, 1.0, 4.0]");
    }

    /**
     * Richer single fixture that drives every read-side scenario in paimon-rust's
     * {@code pk_vector_search_demo} example by varying only the query / filter / read options.
     * Same production write path, indexer, and constraints as {@link
     * #generatePkVectorIvfFlatFixture()}; only the row set differs.
     */
    @Test
    @EnabledIfSystemProperty(named = "gen.pkvector.fixture", matches = "true")
    void generatePkVectorDemoFixture() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()))
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.MERGE_ENGINE.key(), "deduplicate")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .option(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding")
                        .option(
                                "fields.embedding.pk-vector.index.type",
                                IvfFlatVectorGlobalIndexerFactory.IDENTIFIER)
                        .option("fields.embedding.pk-vector.distance.metric", "l2")
                        .option(IvfFlatVectorGlobalIndexerFactory.IDENTIFIER + ".dimension", "2")
                        .option(IvfFlatVectorGlobalIndexerFactory.IDENTIFIER + ".metric", "l2")
                        .option(IvfFlatVectorGlobalIndexerFactory.IDENTIFIER + ".nlist", "1")
                        .build();
        catalog.createTable(identifier(DEMO_TABLE), schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier(DEMO_TABLE));

        InternalRow[] rows = new InternalRow[DEMO_VECTORS.length];
        for (int i = 0; i < DEMO_VECTORS.length; i++) {
            rows[i] = GenericRow.of(i, BinaryVector.fromPrimitiveArray(DEMO_VECTORS[i]));
        }
        write(table, ioManager, rows);
        compact(table, org.apache.paimon.data.BinaryRow.EMPTY_ROW, 0, ioManager, true);

        assertThat(table.latestSnapshot()).isPresent();
        assertThat(table.latestSnapshot().get().indexManifest()).isNotNull();

        String tableLocation = table.location().toString();
        int slash = tableLocation.indexOf('/');
        java.nio.file.Path src = Paths.get(tableLocation.substring(slash));
        java.nio.file.Path dst =
                Paths.get(System.getProperty("user.dir"), "target", "pkvector-fixture", DEMO_TABLE);
        if (Files.exists(dst)) {
            deleteRecursively(dst);
        }
        Files.createDirectories(dst.getParent());
        copyRecursively(src, dst);

        System.out.println("[PkVectorFixtureGenerator] demo fixture written to: " + dst);
        System.out.println(
                "[PkVectorFixtureGenerator] demo vectors (id==rowid): "
                        + java.util.Arrays.deepToString(DEMO_VECTORS));
        System.out.println(
                "[PkVectorFixtureGenerator] demo expected top-3:"
                        + " query [10,0] -> ids [5,1,3];"
                        + " residual id>=3 over [10,0] -> ids [5,3,4];"
                        + " query [0,10] -> ids [4,2,0];"
                        + " query [6,3] -> ids [3,1,5]");
    }

    private static void copyRecursively(java.nio.file.Path src, java.nio.file.Path dst)
            throws Exception {
        try (Stream<java.nio.file.Path> walk = Files.walk(src)) {
            for (java.nio.file.Path p : (Iterable<java.nio.file.Path>) walk::iterator) {
                java.nio.file.Path rel = src.relativize(p);
                java.nio.file.Path target = dst.resolve(rel);
                if (Files.isDirectory(p)) {
                    Files.createDirectories(target);
                } else {
                    Files.createDirectories(target.getParent());
                    Files.copy(p, target, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    private static void deleteRecursively(java.nio.file.Path dir) throws Exception {
        try (Stream<java.nio.file.Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}
