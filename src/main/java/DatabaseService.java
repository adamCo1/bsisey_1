import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;

public class DatabaseService {

    private String username, dbUrl, password ;
    private int titleIndex = 0 , productionYearIndex = 2 ;
    private DatabaseCommunicator databaseCommunicator;
    private Connection connection;
    private final String connectionDriver = "oracle.jdbc.OracleDriver";

    public DatabaseService(String dbUrl, String username, String password){
        this.dbUrl = dbUrl;
        this.username = username;
        this.password = password;
        this.databaseCommunicator = new DatabaseCommunicator();
    }

    public void fileToDataBase(String filePath){
        try {
            this.openDatabaseConnection();
            fillDatabaseTableFrom(filePath);
            closeDatabaseConnection();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public void calculateSimilarity() {
        openDatabaseConnection();
        ArrayList<Long> listOfRecords = getAllMediaITems();
        int maximalDistance = this.getMaximalDistance();
        try {
            for (Long mid1:
                 listOfRecords) {
                for (Long mid2:
                     listOfRecords) {
                    double similarity = getSimCalculation(mid1, mid2, maximalDistance);
                    insertSimilarityToDatabase(mid1, mid2, similarity);
                }
            }
        }catch(Exception e){
            closeDatabaseConnection();
            e.printStackTrace();
        }
        closeDatabaseConnection();
    }

    private ArrayList<Long> getAllMediaITems() {
        ArrayList<Long> listOfRecords = new ArrayList<Long>();
        PreparedStatement statement ;
        String sql = "SELECT MID FROM MediaItems" ;
        try{
            statement = this.connection.prepareStatement(sql);
            ResultSet midsRecords = statement.executeQuery();
            while(midsRecords.next()){
                listOfRecords.add(midsRecords.getLong("MID"));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return listOfRecords;
    }

    private int getMaximalDistance() {
        int maxDistance = 0;
        CallableStatement getMaxProcedure ;
        String sql = "( ? = call MaximalDistance()" ;
        try {
            getMaxProcedure = this.connection.prepareCall(sql);
            getMaxProcedure.registerOutParameter(1, oracle.jdbc.OracleTypes.NUMBER);
            getMaxProcedure.execute();
            maxDistance = getMaxProcedure.getInt(1);
        }catch (Exception e){
            e.printStackTrace();
        }
        return maxDistance;
    }

    private double getSimCalculation(long mid1, long mid2, int maxDistance) {
        double simClaculation = 0;
        CallableStatement getSimCalculation ;
        String sql = "( ? = call simCalculation(?, ?, ?)" ;
        try {
            getSimCalculation = this.connection.prepareCall(sql);
            getSimCalculation.setLong(2, mid1);
            getSimCalculation.setLong(3, mid2);
            getSimCalculation.setInt(4, maxDistance);
            getSimCalculation.registerOutParameter(1, oracle.jdbc.OracleTypes.FLOAT);
            getSimCalculation.execute();
            simClaculation = getSimCalculation.getDouble(1);
        }catch (Exception e){
            e.printStackTrace();
        }
        return simClaculation;
    }

    public void printSimilarItems(long itemId) {
        openDatabaseConnection();
        ArrayList<String> similarTitles = this.selectAllSimilarity(itemId);
        for (String title: similarTitles
             ) {
            System.out.println(title);

        }
        closeDatabaseConnection();

    }

    private ArrayList<String> selectAllSimilarity(long mid){
        ArrayList<String> as = new ArrayList<String>();
        PreparedStatement statement;
        String sql = "SELECT MEDIAITEMS.TITLE,SIMILIARITY.MID2,SIMILIARITY.SIMILIARITY FROM INNER JOIN MEDIAITEMS ON SIMILIARITY.MID2 WHERE MID1=? AND SIMILIARITY.SIMILIARITY >= 0.3 ORDER BY SIMILARITY DESC";
        try{
            statement = this.connection.prepareStatement(sql);
            statement.setLong(1,mid);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()){
                as.add(resultSet.getString(1));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return as;
    }



    private void insertSimilarityToDatabase(long mid1, long mid2, double similarity) throws SQLException {
        PreparedStatement statement;
        String sql = "INSERT INTO Similarity VALUES(?, ?, ?)";
        try{
            statement = this.connection.prepareStatement(sql);
            statement.setLong(1, mid1);
            statement.setLong(2, mid2);
            statement.setDouble(3, similarity);
            statement.execute();
            this.connection.commit();
        }catch (Exception e){
            this.connection.rollback();
        }
    }


    public void fillDatabaseTableFrom(String filePath) throws IOException {
        this.openDatabaseConnection();
        String row, title= "";
        int prod_yaer = 0;
        BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
        while((row = bufferedReader.readLine()) != null){
            String[] records = row.split(",");
            for(int i = 0 ; i < records.length ; i += 2) {
                try{
                    title = records[i];
                    prod_yaer = Integer.parseInt(records[i+1]);
                    insertRecord(title,prod_yaer);
                }catch (SQLException e){
                    System.out.println("couldnt add record " + title + " , " + prod_yaer);
                }
            }
        }
        this.closeDatabaseConnection();
    }

    private void insertRecord(String title, int prod_year) throws SQLException {
        PreparedStatement statement ;
        try{
            String sql = "insert into MediaItems(TITLE, PROD_YEAR)" +
                    " VALUES(?,?)";
            statement = this.connection.prepareStatement(sql);
            statement.setString(1, title);
            statement.setInt(2, prod_year);
            statement.execute();
            this.connection.commit();
        }catch (Exception e){
            this.connection.rollback();
        }
    }




    public void openDatabaseConnection() {
        try{
            Class.forName(this.connectionDriver);
            connection = DriverManager.getConnection(dbUrl, username, password);
            connection.setAutoCommit(false);
        }catch (Exception e) {
            System.out.println("cant connect to databasePath = [" + dbUrl + "], username = [" + username + "], password = [" + password + "]");
            e.printStackTrace();
        }
    }

    public void closeDatabaseConnection() {
        try{
            this.connection.close();
        }catch (SQLException e){
            System.out.println("cant close connection to data base");
            e.printStackTrace();
        }

    }

}