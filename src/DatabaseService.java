import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DatabaseService {

    private String connectionPath, databaseUser, databasePassword;
    private int titleIndex = 0 , productionYearIndex = 2 ;
    private DatabaseCommunicator databaseCommunicator;

    public DatabaseService(String connectionPath, String databaseUser, String databasePassword){
        this.connectionPath = connectionPath;
        this.databasePassword = databasePassword;
        this.databaseUser = databaseUser;
        this.databaseCommunicator = new DatabaseCommunicator(connectionPath, databaseUser, databasePassword);
    }

    public void fileToDataBase(String filePath){
        try {
            fillDatabaseTableFrom(filePath);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void fillDatabaseTableFrom(String filePath) throws IOException {
        String row ;
        BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
        while((row = bufferedReader.readLine()) != null){
            String[] record = row.split(",");
            this.databaseCommunicator.insertMediaItem(record);
        }
    }

    public void calculateSimilarity() {
        int maximalDistance = this.databaseCommunicator.getMaximalDistance();
        ResultSet resultSetOne = this.databaseCommunicator.getMediaItemsTable();
        ResultSet resultSetTwo = this.databaseCommunicator.getMediaItemsTable();
        try {
            while (resultSetTwo.next()) {
                int prodYear1 = resultSetOne.getInt(this.productionYearIndex);
                int mid1 = resultSetOne.getInt(0);
                while(resultSetTwo.next()){
                    int prodYear2 = resultSetTwo.getInt(this.productionYearIndex);
                    int mid2 = resultSetTwo.getInt(0);
                    int itemsSimilarity = this.databaseCommunicator.calculateSimilarity(prodYear1, prodYear2, maximalDistance);
                    this.databaseCommunicator.insertSimilarity(mid1, mid2, itemsSimilarity);
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public void printSimilarItems(long itemId) {
        float criterion = 0.3f;
        String order = "ASCENDING";
        ResultSet resultSet = this.databaseCommunicator.getItemsBySimilarity(criterion, order);

        try{
            while(resultSet.next()){
                int mid1 = resultSet.getInt(0);
                int mid2 = resultSet.getInt(1);
                float similarity = resultSet.getFloat(2);
                String title1 = this.databaseCommunicator.getItemTitle(mid1);
                String title2 = this.databaseCommunicator.getItemTitle(mid2);
                System.out.println("title : " + title1 + " title : " + title2 + " similarity : " + similarity);
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}