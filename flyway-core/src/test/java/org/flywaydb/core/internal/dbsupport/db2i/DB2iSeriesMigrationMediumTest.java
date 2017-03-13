/**
 * Copyright 2010-2016 Boxfuse GmbH
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.internal.dbsupport.db2i;

import org.flywaydb.core.DbCategory;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.internal.dbsupport.Schema;
import org.flywaydb.core.internal.util.jdbc.DriverDataSource;
import org.flywaydb.core.migration.MigrationTestCase;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(DbCategory.DB2iSeries.class)
public class DB2iSeriesMigrationMediumTest extends MigrationTestCase {

    public static final String TEST_SCHEMA = "TESTADM";
    private String query;

    @Override
    protected void configureFlyway() {
        super.configureFlyway();
        flyway.setTable("schema_version");
        flyway.setSchemas(TEST_SCHEMA);
        flyway.setBaselineOnMigrate(true);
    }

    @Override
    protected DataSource createDataSource(Properties customProperties) throws Exception {
        String user = customProperties.getProperty("db2i.user", "user");
        String password = customProperties.getProperty("db2i.password", "password");
        String url = customProperties.getProperty("db2i.url", "jdbc:as400://host:port/schemaname");

        return new DriverDataSource(Thread.currentThread().getContextClassLoader(), null, url, user, password);
    }

    @Override
    protected String getQuoteLocation() {
        return "migration/quote";
    }

    @Test
    public void init() throws Exception {

        query = "SELECT COUNT(*) FROM SYSIBM.TABLES WHERE TABLE_SCHEMA = '" + TEST_SCHEMA + "' AND TABLE_TYPE='BASE TABLE'";

        flyway.baseline();
        int countTablesAfterInit = jdbcTemplate.queryForInt(query);
        assertEquals(1, countTablesAfterInit);
    }

    @Test
    public void migrateCRUD() throws Exception {

        query = "select count(*) from sysibm.tables where table_schema = '" + TEST_SCHEMA + "' AND TABLE_TYPE='BASE TABLE'";

        flyway.setLocations("migration/dbsupport/db2i/sql/crud");

        int countTablesBeforeMigration = jdbcTemplate.queryForInt(query);
        assertEquals(0, countTablesBeforeMigration);
        flyway.baseline();
        flyway.migrate();
        int countTablesAfterMigration = jdbcTemplate.queryForInt(query);
        assertEquals(2, countTablesAfterMigration);

        MigrationVersion version = flyway.info().current().getVersion();
        assertEquals("1.2", version.toString());
        assertEquals("UpdateTable", flyway.info().current().getDescription());
        assertEquals("Nils", jdbcTemplate.queryForString("select firstname from " + TEST_SCHEMA + ".PERSON where lastname = 'Nilsen'"));

    }

    @Test
    public void sequence() throws Exception {
        flyway.setLocations("migration/dbsupport/db2i/sql/sequence");
        flyway.baseline();
        flyway.migrate();

        MigrationVersion migrationVersion = flyway.info().current().getVersion();
        assertEquals("1.1", migrationVersion.toString());
        assertEquals("Sequence", flyway.info().current().getDescription());
        assertEquals(666, jdbcTemplate.queryForInt("SELECT NEXTVAL FOR " + TEST_SCHEMA + ".BEAST_SEQ from sysibm.sysdummy1"));
    }

    @Test
    public void type() throws Exception {
        flyway.setLocations("migration/dbsupport/db2i/sql/type");
        flyway.baseline();
        flyway.migrate();
    }

    @Test
    public void view() throws Exception {
        flyway.setLocations("migration/dbsupport/db2i/sql/view");
        flyway.baseline();
        flyway.migrate();
    }

    @Test
    public void trigger() throws Exception {
        flyway.setLocations("migration/dbsupport/db2i/sql/trigger");
        flyway.baseline();
        flyway.migrate();

        jdbcTemplate.execute("INSERT INTO EMPLOYEE(ID , NAME) VALUES (1, 'Gene')");
        jdbcTemplate.execute("INSERT INTO EMPLOYEE(ID , NAME) VALUES (2, 'Simmons')");
        assertTrue(jdbcTemplate.queryForInt("SELECT COUNT(*) FROM COMPANY_STATS") == 2);
    }

    @Test
    public void routines() throws Exception {
        flyway.setLocations("migration/dbsupport/db2i/sql/routines");
        flyway.baseline();
        flyway.migrate();
    }


    @Test
    public void alias() throws Exception {
        flyway.setLocations("migration/dbsupport/db2i/sql/alias");
        flyway.baseline();
        flyway.migrate();
    }


    /**
     * Override schema test. db2 on zOS does not support "Create schema"
     *
     * @throws Exception
     */
    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void setCurrentSchema() throws Exception {
        Schema schema = dbSupport.getSchema("current_schema_test");
        try {
            schema.drop();
            schema.create();

            flyway.setSchemas("current_schema_test");
            flyway.clean();

            flyway.setLocations("migration/current_schema");
            Map<String, String> placeholders = new HashMap<String, String>();
            placeholders.put("schema1", dbSupport.quote("current_schema_test"));
            flyway.setPlaceholders(placeholders);
            flyway.migrate();
        } finally {
            schema.drop();
        }
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void migrateMultipleSchemas() throws Exception {
        flyway.setSchemas("flyway_1", "flyway_2", "flyway_3");
        flyway.clean();

        flyway.setLocations("migration/multi");
        Map<String, String> placeholders = new HashMap<String, String>();
        placeholders.put("schema1", dbSupport.quote("flyway_1"));
        placeholders.put("schema2", dbSupport.quote("flyway_2"));
        placeholders.put("schema3", dbSupport.quote("flyway_3"));
        flyway.setPlaceholders(placeholders);
        flyway.migrate();
        assertEquals("2.0", flyway.info().current().getVersion().toString());
        assertEquals("Add foreign key", flyway.info().current().getDescription());
        assertEquals(0, flyway.migrate());

        assertEquals(4, flyway.info().applied().length);
        assertEquals(2, jdbcTemplate.queryForInt("select count(*) from " + dbSupport.quote("flyway_1") + ".test_user1"));
        assertEquals(2, jdbcTemplate.queryForInt("select count(*) from " + dbSupport.quote("flyway_2") + ".test_user2"));
        assertEquals(2, jdbcTemplate.queryForInt("select count(*) from " + dbSupport.quote("flyway_3") + ".test_user3"));

        flyway.clean();
    }


    @Test
    public void nonEmptySchemaWithDisableInitCheck() throws Exception {
        jdbcTemplate.execute("CREATE TABLE t1 (\n" +
                "  name VARCHAR(25) NOT NULL,\n" +
                "  PRIMARY KEY(name))");

        flyway.setLocations(BASEDIR);
        flyway.setBaselineVersionAsString("0_1");
        flyway.setBaselineOnMigrate(false);
        flyway.baseline();
        flyway.migrate();
    }

    /**
     * Check validation with INIT row.
     */
    @Override
    @Test
    public void checkValidationWithInitRow() throws Exception {
        flyway.setLocations(BASEDIR);
        flyway.setTarget(MigrationVersion.fromVersion("1.1"));
        flyway.migrate();
        assertEquals("1.1", flyway.info().current().getVersion().toString());

        jdbcTemplate.update("DROP TABLE " + dbSupport.quote(flyway.getTable()));
        jdbcTemplate.update("DROP TABLESPACE " + dbSupport.quote(flyway.getSchemas()) + ".SDBVERS");
        flyway.setBaselineVersionAsString("1.1");
        flyway.setBaselineDescription("initial version 1.1");
        flyway.baseline();

        flyway.setTarget(MigrationVersion.LATEST);
        flyway.migrate();
        assertEquals("2.0", flyway.info().current().getVersion().toString());
        flyway.validate();
    }


}
