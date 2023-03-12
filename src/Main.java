import java.io.IOException;

public class Main {
public static void main(String[] args) throws IOException, InterruptedException {

        DataTransformer EltTransformer = new DataTransformer();
        String rawFilePath = EltTransformer.getAndLoadData("https://fakestoreapi.com/products");
        String transformedFilePath = EltTransformer.transformData(rawFilePath);
        EltTransformer.loadCsvToBigQuery("sales", "Products", transformedFilePath);

}
}