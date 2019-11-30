import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;

public class DatabaseService {

    private String username, dbUrl, password ;
    private Connection connection;
    private final String connectionDriver = "oracle.jdbc.OracleDriver";

    public DatabaseService(String dbUrl, String username, String password){
        this.dbUrl = dbUrl;
        this.username = username;
        this.password = password;
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
            for (Long firstRecordMid:
                 listOfRecords) {
                for (Long secondRecordMid:
                     listOfRecords) {
                    float similarity = getSimCalculation(firstRecordMid, secondRecordMid, maximalDistance);
                    if(firstRecordMid != secondRecordMid) {
                        insertSimilarityToDatabase(firstRecordMid, secondRecordMid, similarity);
                    }
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
        String sql = "{ ? = call MaximalDistance()}" ;
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

    private float getSimCalculation(long mid1, long mid2, int maxDistance) {
        float simClaculation = 0;
        CallableStatement getSimCalculation ;
        String sql = "{ ? = call simCalculation(?, ?, ?)}" ;
        try {
            getSimCalculation = this.connection.prepareCall(sql);
            getSimCalculation.setLong(2, mid1);
            getSimCalculation.setLong(3, mid2);
            getSimCalculation.setInt(4, maxDistance);
            getSimCalculation.registerOutParameter(1, oracle.jdbc.OracleTypes.FLOAT);
            getSimCalculation.execute();
            simClaculation = getSimCalculation.getFloat(1);
        }catch (Exception e){
            e.printStackTrace();
        }
        return simClaculation;
    }

    public void printSimilarItems(long itemId) {
        openDatabaseConnection();
        try{
            ArrayList<SimilarityDto> similarItems = this.selectAllSimilarity(itemId);
            for (SimilarityDto dto:
                 similarItems) {
                System.out.print(dto.getTitle() + " ");
                System.out.println(dto.getSimilarity());
            }
        }catch (Exception e){
            closeDatabaseConnection();
            e.printStackTrace();
        }
        closeDatabaseConnection();
    }

    private ArrayList<SimilarityDto> selectAllSimilarity(long mid){
        ArrayList<SimilarityDto> listOfItems = new ArrayList<SimilarityDto>();
        PreparedStatement statement;
        String sql = "SELECT MediaItems.TITLE, Similarity.MID2, Similarity.SIMILARITY " +
                "FROM Similarity INNER JOIN MediaItems ON Similarity.MID2 = MediaItems.MID " +
                "WHERE MID1 = ? AND Similarity.SIMILARITY >= 0.3 " +
                "ORDER BY SIMILARITY DESC ;";
        try{
            statement = this.connection.prepareStatement(sql);
            statement.setLong(1,mid);
            ResultSet resultSet = statement.executeQuery();
            listOfItems = fillSimilarityList(resultSet);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return listOfItems;
    }

    private ArrayList<SimilarityDto> fillSimilarityList(ResultSet fromResult) throws SQLException {
        ArrayList<SimilarityDto> dtoList = new ArrayList<SimilarityDto>();
        while (fromResult.next()){
            SimilarityDto similarityDto = new SimilarityDto(fromResult.getString(1), fromResult.getFloat(3));
            dtoList.add(similarityDto);
        }

        return dtoList ;
    }

    private void insertSimilarityToDatabase(long mid1, long mid2, float similarity) throws SQLException {
        PreparedStatement statement;
        String sql = "INSERT INTO Similarity VALUES(?, ?, ?)";
        try{
            statement = this.connection.prepareStatement(sql);
            statement.setLong(1, mid1);
            statement.setLong(2, mid2);
            statement.setFloat(3, similarity);
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




    private void openDatabaseConnection() {
        try{
            Class.forName(this.connectionDriver);
            connection = DriverManager.getConnection(dbUrl, username, password);
            connection.setAutoCommit(false);
        }catch (Exception e) {
            System.out.println("cant connect to databasePath = [" + dbUrl + "], username = [" + username + "], password = [" + password + "]");
            e.printStackTrace();
        }
    }

    private void closeDatabaseConnection() {
        try{
            this.connection.close();
        }catch (SQLException e){
            System.out.println("cant close connection to data base");
            e.printStackTrace();
        }

    }

}