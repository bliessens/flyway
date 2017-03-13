import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfoService;
import org.flywaydb.core.internal.info.MigrationInfoDumper;
import org.junit.Before;
import org.junit.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.util.Properties;

public class RunFlyway {

    private final Flyway fw = new Flyway();
    private final Properties props = new Properties();

    @Before
    public void setUp() throws Exception {
        props.put("flyway.url", "jdbc:as400://172.30.52.130/BENOIT3;date format=iso");
        props.put("flyway.user", "LIMSERVER");
        props.put("flyway.password", "SERVERLIM");
        fw.configure(props);

    }

    @Test
    public void runFlywayClean() throws Exception {
        fw.clean();
    }

    @Test
    public void runFlywayInfo() throws Exception {
        final MigrationInfoService info = fw.info();
        System.out.println(MigrationInfoDumper.dumpToAsciiTable(info.all()));
    }

    @Test
    public void runFlywayValidate() throws Exception {
        fw.validate();
    }

    @Test
    public void runFlywayBaseline() throws Exception {
        fw.baseline();
    }

    @Test
    public void runFlywayMigrate() throws Exception {
        final int migrate = fw.migrate();
        System.out.println("Migration done: " + migrate);
    }

    @Test
    public void checkPresenceOfTAble() throws Exception {

        final Connection connection = fw.getDataSource().getConnection();
        connection.setAutoCommit(false);
        CallableStatement cs = connection.prepareCall("CREATE SCHEMA BENOIT3");
        cs.execute();
        cs = connection.prepareCall("CREATE TABLE \"BENOIT3\".\"schema_version\" (" +
                "    \"installed_rank\" INT NOT NULL," +
                "    \"version\" VARCHAR(50)," +
                "    \"description\" VARCHAR(200) NOT NULL," +
                "    \"type\" VARCHAR(20) NOT NULL," +
                "    \"script\" VARCHAR(1000) NOT NULL," +
                "    \"checksum\" INT," +
                "    \"installed_by\" VARCHAR(100) NOT NULL," +
                "    \"installed_on\" TIMESTAMP DEFAULT CURRENT TIMESTAMP NOT NULL," +
                "    \"execution_time\" INT NOT NULL," +
                "    \"success\" SMALLINT NOT NULL," +
                "    CONSTRAINT \"BENOIT3_s\" CHECK (\"success\" in(0,1))" +
                ") ");
        cs.execute();
        boolean found = connection.getMetaData().getTables(null, "BENOIT3", "schema_version", null).next();
        found = connection.getMetaData().getTables(null, "BENOIT3", "\"schema_version\"", null).next();
        connection.rollback();
//        connection.commit();
    }
}
