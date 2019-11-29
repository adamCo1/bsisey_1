import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class DatabaseCommunicator {

    private String dbUrl , username, password;
    private final String connectionDriver = "oracle.jdbc.driver.OracleDriver";

    public DatabaseCommunicator(){
        this.dbUrl = "132.72.65.216";
        this.password= "abcd";
        this.username = "adamcohe" ;
    }

    public Connection getConnection() {
        Connection connection = null ;
        try{
            Class.forName(this.connectionDriver);
            connection = DriverManager.getConnection(dbUrl, username, password);
            connection.setAutoCommit(false);
            return connection ;
        }catch (Exception e) {
            System.out.println("cant connect to databasePath = [" + dbUrl + "], username = [" + username + "], password = [" + password + "]");
            e.printStackTrace();
        }

        return connection ;
    }




}
