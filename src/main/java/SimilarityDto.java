public class SimilarityDto {

    private String title ;
    private float similarity ;

    public SimilarityDto(String title, float similarity) {
        this.title = title;
        this.similarity = similarity;
    }

    public String getTitle() {
        return this.title;
    }

    public float getSimilarity() {
        return this.similarity;
    }
}
