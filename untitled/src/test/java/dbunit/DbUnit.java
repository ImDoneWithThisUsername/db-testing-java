package dbunit;
import org.dbunit.DBTestCase;
import org.dbunit.PropertiesBasedJdbcDatabaseTester;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.ext.mysql.MySqlMetadataHandler;
import org.dbunit.operation.DatabaseOperation;
import org.junit.Test;

import java.io.FileInputStream;
import java.sql.*;

public class DbUnit extends DBTestCase {
    Connection connect = null;
    Statement statement = null;

    public DbUnit(String name) {
        super(name);
        System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_DRIVER_CLASS, "com.mysql.cj.jdbc.Driver");
        System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_CONNECTION_URL, "jdbc:mysql://localhost:3306/orangehrm");
        System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_USERNAME, "root");
        System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_PASSWORD, "");
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connect = DriverManager.getConnection("jdbc:mysql://localhost:3306/orangehrm", "root", "");
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected IDataSet getDataSet() throws Exception {
        return new FlatXmlDataSetBuilder().build(new FileInputStream("data.xml"));
    }

    protected DatabaseOperation getSetUpOperation()throws Exception{
        return DatabaseOperation.REFRESH;
    }

    protected DatabaseOperation getTearDownOperation()throws Exception{
        return DatabaseOperation.NONE;
    }

    @Override
    protected void setUpDatabaseConfig(DatabaseConfig config) {
        config.setProperty(DatabaseConfig.PROPERTY_BATCH_SIZE, 97);
        config.setProperty(DatabaseConfig.PROPERTY_METADATA_HANDLER, new MySqlMetadataHandler());
        config.setFeature(DatabaseConfig.FEATURE_BATCHED_STATEMENTS, true);
    }

    @Test
    public void testEmployeeLeaveOver12Days() throws SQLException {
        statement = connect.createStatement();


        String sqlString =
                "select count(*) "+
                "from " +
                        "(" +
                        "select distinct e1.emp_number " +
                        "from hs_hr_employee e1 " +
                            "join ohrm_leave_request lr1 on e1.emp_number = lr1.emp_number " +
                            "join ohrm_leave l1 on l1.leave_request_id = lr1.id " +
                        "where year(l1.date) = 2022 " +
                        "group by e1.emp_number " +
                        "having sum(l1.length_days) > 12 " +
                        ") as temp";

        ResultSet res = statement.executeQuery(sqlString);

        System.out.println("So luong ung vien nghi hon 12 ngay trong nam: ");
        if(res.next()){
            System.out.println(res.getString(1));
        }
    }

    ;
}

//    String sqlString =
//            "select count(*) " +
//                    "from ohrm_leave where length_days > 12 and year(date) = 2022";
