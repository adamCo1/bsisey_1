public class Main {

    public static void main(String[] args) {
        DatabaseService databaseService = new DatabaseService();
        String dataPath = "films.csv";
        databaseService.fileToDataBase(dataPath);
    }
}
