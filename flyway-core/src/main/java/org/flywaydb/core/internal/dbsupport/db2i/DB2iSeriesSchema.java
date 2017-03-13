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

import org.flywaydb.core.internal.dbsupport.*;
import org.flywaydb.core.internal.util.FileCopyUtils;

import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * DB2 for iSeries implementation of Schema.
 */
public class DB2iSeriesSchema extends Schema<DB2iSeriesDbSupport> {
    /**
     * Creates a new DB2i schema.
     *
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param dbSupport    The database-specific support.
     * @param name         The name of the schema.
     */
    public DB2iSeriesSchema(JdbcTemplate jdbcTemplate, DB2iSeriesDbSupport dbSupport, String name) {
        super(jdbcTemplate, dbSupport, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForInt("SELECT COUNT(*) FROM SYSIBM.SCHEMATA WHERE SCHEMA_NAME=?", name) > 0;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        int objectCount = jdbcTemplate.queryForInt("select count(*) from sysibm.tables where table_schema = ?", name);
        objectCount += jdbcTemplate.queryForInt("select count(*) from sysibm.views where table_schema = ?", name);
//            objectCount += jdbcTemplate.queryForInt("select count(*) from sysibm.sequences where seqschema = ?", name);
//            objectCount += jdbcTemplate.queryForInt("select count(*) from sysibm.indexes where indschema = ?", name);
        objectCount += jdbcTemplate.queryForInt("select count(*) from sysibm.sqlprocedures where procedure_schem = ?", name);
        objectCount += jdbcTemplate.queryForInt("select count(*) from sysibm.sqlfunctions where function_schem = ?", name);
//            objectCount += jdbcTemplate.queryForInt("select count(*) from sysibm.triggers where trigschema = ?", name);
        return objectCount == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        jdbcTemplate.execute("CREATE SCHEMA " + dbSupport.quote(name));
    }

    @Override
    protected void doDrop() throws SQLException {
        clean();
        // note: DROP SCHEMA only supported when commit level is NONE and when SCHEMA is not in LibraryList
//        jdbcTemplate.execute("CALL QSYS.QCMDEXC('ADDRPYLE SEQNBR(8000) MSGID(CPA7025) RPY(''I'')', 0000000045.00000)");
//        jdbcTemplate.execute("CALL QSYS.QCMDEXC('CHGJOB INQMSGRPY(*SYSRPYL)', 0000000026.00000)");
//        jdbcTemplate.execute("DROP SCHEMA " + dbSupport.quote(name) + " RESTRICT");
    }

    @Override
    protected void doClean() throws SQLException {
        // views
        for (String dropStatement : generateDropStatementsForViews()) {
            jdbcTemplate.execute(dropStatement);
        }

        // aliases
        for (String dropStatement : generateDropStatements("ALIAS")) {
            jdbcTemplate.execute(dropStatement);
        }

        for (Table table : allTables()) {
            table.drop();
        }

        // sequences
        for (String dropStatement : generateDropStatementsForSequences()) {
            jdbcTemplate.execute(dropStatement);
        }

        // procedures
        for (String dropStatement : generateDropStatementsForProcedures()) {
            jdbcTemplate.execute(dropStatement);
        }

        // triggers
        for (String dropStatement : generateDropStatementsForTriggers()) {
            jdbcTemplate.execute(dropStatement);
        }

        for (Function function : allFunctions()) {
            function.drop();
        }

        for (Type type : allTypes()) {
            type.drop();
        }
    }


    /**
     * Generates DROP statements for the procedures in this schema.
     *
     * @return The drop statements.
     * @throws SQLException when the statements could not be generated.
     */
    private List<String> generateDropStatementsForProcedures() throws SQLException {
        String findProcQuery;
        try {
            final InputStreamReader in = new InputStreamReader(DB2iSeriesSchema.class.getClassLoader().getResourceAsStream("org/flywaydb/core/internal/dbsupport/db2i/select_procedures.sql"));
            findProcQuery = FileCopyUtils.copyToString(in);
        } catch (Exception e) {
            throw new SQLException("Could not load SQL file", e);
        }
        final List<Map<String, String>> resultMap = jdbcTemplate.queryForList(findProcQuery, this.name, this.name);
        List<String> dropProcStatements = new ArrayList<String>(resultMap.size());
        for (Map<String, String> map : resultMap) {
            dropProcStatements.add(String.format("DROP PROCEDURE %s ( %s )", map.get("PROCEDURE_NAME"), map.get("PARAM_LIST")));
        }
        return dropProcStatements;
    }

    /**
     * Generates DROP statements for the triggers in this schema.
     *
     * @return The drop statements.
     * @throws SQLException when the statements could not be generated.
     */
    private List<String> generateDropStatementsForTriggers() throws SQLException {
        final String dropTrigGenQuery = "SELECT TRIGGER_NAME from QSYS2.SYSTRIGGERS WHERE TRIGGER_SCHEMA = '" + name + "'";
        return buildDropStatements("DROP TRIGGER", dropTrigGenQuery);
    }

    /**
     * Generates DROP statements for the sequences in this schema.
     *
     * @return The drop statements.
     * @throws SQLException when the statements could not be generated.
     */
    private List<String> generateDropStatementsForSequences() throws SQLException {
        final String dropSeqGenQuery = "SELECT SYSTEM_SEQ_NAME from QSYS2.SYSSEQUENCES WHERE SEQUENCE_SCHEMA = '" + name + "'";
        return buildDropStatements("DROP SEQUENCE", dropSeqGenQuery);
    }

    /**
     * Generates DROP statements for the views in this schema.
     *
     * @return The drop statements.
     * @throws SQLException when the statements could not be generated.
     */
    private List<String> generateDropStatementsForViews() throws SQLException {
        final String dropSeqGenQuery = "select TABLE_NAME from SYSIBM.VIEWS where TABLE_SCHEMA = '" + name
                + "' AND TABLE_NAME NOT IN ('SYSTABLES', 'SYSINDEXES', 'SYSCOLUMNS', 'SYSKEYS'," +
                " 'SYSVIEWDEP', 'SYSVIEWS', 'SYSPACKAGE', 'SYSCST', 'SYSREFCST', 'SYSKEYCST'," +
                " 'SYSCSTDEP', 'SYSCSTCOL', 'SYSCHKCST', 'SYSTRIGGERS', 'SYSTRIGDEP', 'SYSTRIGCOL'," +
                " 'SYSTRIGUPD', 'SYSTABLEDEP', 'SYSFIELDS')";
        return buildDropStatements("DROP VIEW", dropSeqGenQuery);
    }

    /**
     * Generates DROP statements for this type of table, representing this type of object in this schema.
     *
     * @param objectType The type of object.
     * @return The drop statements.
     * @throws SQLException when the statements could not be generated.
     */
    private List<String> generateDropStatements(String objectType) throws SQLException {
        final String dropTablesGenQuery = "SELECT TABLE_NAME FROM SYSIBM.TABLES where TABLE_TYPE='" + objectType + "' and TABLE_SCHEMA = '"
                + name + "'";
        return buildDropStatements("DROP " + objectType, dropTablesGenQuery);
    }

    /**
     * Builds the drop statements for database objects in this schema.
     *
     * @param dropPrefix The drop command for the database object (e.g. 'drop table').
     * @param query      The query to get all present database objects
     * @return The statements.
     * @throws SQLException when the drop statements could not be built.
     */
    private List<String> buildDropStatements(final String dropPrefix, final String query) throws SQLException {
        List<String> dropStatements = new ArrayList<String>();
        List<String> dbObjects = jdbcTemplate.queryForStringList(query);
        for (String dbObject : dbObjects) {
            dropStatements.add(dropPrefix + " " + dbObject);
        }
        return dropStatements;
    }

    private Table[] findTables(String sqlQuery, String... params) throws SQLException {
        List<String> tableNames = jdbcTemplate.queryForStringList(sqlQuery, params);
        Table[] tables = new Table[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            tables[i] = new DB2iSeriesTable(jdbcTemplate, dbSupport, this, tableNames.get(i));
        }
        return tables;
    }

    @Override
    protected Table[] doAllTables() throws SQLException {
        return findTables("SELECT TABLE_NAME FROM SYSIBM.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'", name);
    }

    @Override
    protected Function[] doAllFunctions() throws SQLException {
        final String dropFuncQuery = "SELECT SPECIFIC_NAME FROM SYSIBM.SQLFUNCTIONS WHERE FUNCTION_SCHEM = '" + name + "'";

        List<Function> functions = new ArrayList<Function>();
        for (String specificName : jdbcTemplate.queryForStringList(dropFuncQuery)) {
            functions.add(getFunction(specificName));
        }

        return functions.toArray(new Function[functions.size()]);
    }

    @Override
    public Table getTable(String tableName) {
        return new DB2iSeriesTable(jdbcTemplate, dbSupport, this, tableName);
    }

    @Override
    protected Type getType(String typeName) {
        return new DB2iSeriesType(jdbcTemplate, dbSupport, this, typeName);
    }

    @Override
    public Function getFunction(String functionName, String... args) {
        return new DB2iSeriesFunction(jdbcTemplate, dbSupport, this, functionName, args);
    }
}
