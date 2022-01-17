package dk.ku.di.dms.vms.database;

import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.jdbcx.JdbcDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class H2ConnectionManager {

    private final JdbcConnectionPool cp;

    public H2ConnectionManager() {
        JdbcDataSource dataSource = new org.h2.jdbcx.JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:db1;DB_CLOSE_DELAY=-1");
        this.cp = JdbcConnectionPool.create(dataSource);
    }

    //    public Connection getConnection() throws SQLException {
//        Driver driver = org.h2.Driver.load();
//        // http://h2database.com/html/features.html
//        // http://h2database.com/html/features.html?highlight=AUTO_SERVER&search=AUTO_SERVER#in_memory_databases
//        Connection conn = driver.connect("jdbc:h2:mem:db1;DB_CLOSE_DELAY=-1",null);
//        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
//        conn.setAutoCommit(false);
//        return conn;
//    }

    // about the use of a connection per thread
    // https://stackoverflow.com/questions/66966026/can-multiple-threads-share-the-same-database-connection-to-execute-multiple-prep

    // TODO test with Hikari later on
    //  https://github.com/brettwooldridge/HikariCP
    public Connection getConnection() throws SQLException {
        Connection conn = cp.getConnection();
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        return conn;
    }

    public void shutdown() throws SQLException {

        Connection conn = getConnection();
        PreparedStatement ps2 = conn.prepareStatement("SHUTDOWN;");
        ResultSet rs = ps2.executeQuery();

    }

}
