import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {

        String runTime = DataTransformer.getRuntime();
        String rawFilePath = DataTransformer.getAndLoadData("https://fakestoreapi.com/products",runTime);
        String transformedFilePath = DataTransformer.transformData(rawFilePath,runTime);
        DataTransformer.loadCsvToBigQuery("sales", "Products", transformedFilePath);
    }
}