import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        DatabaseService databaseService = new DatabaseService("jdbc:oracle:thin:@132.72.65.216:1521:oracle","adamcohe","abcd");
        String dataPath = "films.csv";
//        databaseService.fillDatabaseTableFrom("films.csv");
        databaseService.calculateSimilarity();
        databaseService.printSimilarItems(2);
    }
}
