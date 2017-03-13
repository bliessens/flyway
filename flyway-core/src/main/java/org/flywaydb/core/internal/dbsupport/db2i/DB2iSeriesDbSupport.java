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

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.dbsupport.DbSupport;
import org.flywaydb.core.internal.dbsupport.JdbcTemplate;
import org.flywaydb.core.internal.dbsupport.Schema;
import org.flywaydb.core.internal.dbsupport.SqlStatementBuilder;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

/**
 * DB2 for iSeries Support.
 */
public class DB2iSeriesDbSupport extends DbSupport {
    /**
     * The major version of DB2. (9, 10, ...)
     */
    private final int majorVersion;

    /**
     * Creates a new instance.
     *
     * @param connection The connection to use.
     */
    public DB2iSeriesDbSupport(Connection connection) {
        super(new JdbcTemplate(connection, Types.VARCHAR));
        System.out.println("Using " + DB2iSeriesDbSupport.class.getSimpleName());
        try {
            majorVersion = connection.getMetaData().getDatabaseMajorVersion();
        } catch (SQLException e) {
            throw new FlywayException("Unable to determine DB2 for iSeries major version", e);
        }
    }

    public SqlStatementBuilder createSqlStatementBuilder() {
        return new DB2iSeriesSqlStatementBuilder();
    }

    public String getDbName() {
        return "db2i";
    }

    @Override
    protected String doGetCurrentSchemaName() throws SQLException {
        return jdbcTemplate.queryForString("SELECT CURRENT_SCHEMA FROM SYSIBM.SYSDUMMY1");
    }

    @Override
    protected void doChangeCurrentSchemaTo(String schema) throws SQLException {
        jdbcTemplate.execute("SET SCHEMA " + schema);
    }

    public String getCurrentUserFunction() {
        try {
            return jdbcTemplate.queryForString("SELECT '''' || USER  || '''' FROM SYSIBM.SYSDUMMY1");
        } catch (SQLException e) {
            throw new FlywayException("Unable to retrieve the current user for the connection", e);
        }
    }

    public boolean supportsDdlTransactions() {
        return true;
    }

    public String getBooleanTrue() {
        return "1";
    }

    public String getBooleanFalse() {
        return "0";
    }

    @Override
    public String doQuote(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public Schema getSchema(String name) {
        return new DB2iSeriesSchema(jdbcTemplate, this, name);
    }

    @Override
    public boolean catalogIsSchema() {
        return false;
    }

    /**
     * @return The major version of DB2. (9, 10, ...)
     */
    public int getDb2MajorVersion() {
        return majorVersion;
    }
}
