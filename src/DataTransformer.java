import com.google.cloud.bigquery.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.Channels;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * DataTransformer Class used in ELT to BigQuery
 * @author Victor Iwuoha.
 *
 */
public class DataTransformer {

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();


    private static String getRuntime() {
        // Used to get Runtime Info when Class is instantiated
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
        Date date = new Date();
        return formatter.format(date);
    }

    private static final String runTimeInfo = getRuntime();

    /**
     * Makes Call to an  API to retrieve data and stores to filepath.
     * @param apiEndpoint API Endpoint to call for Json Data.
     * @return filepath to Json Data.
     */
    public String getAndLoadData(String apiEndpoint) {
        HttpClient client = HttpClient.newHttpClient();
        JsonArray jsonResponseArray;
        try {
            HttpRequest apiRequest = HttpRequest.newBuilder()
                    .uri(URI.create(apiEndpoint))
                    .build();
            System.out.println("=> Making Request to -> " + apiEndpoint);
            HttpResponse<String> apiResponse = client.send(apiRequest, HttpResponse.BodyHandlers.ofString());
            //  Transform Response to JsonArray to get size
            jsonResponseArray = gson.fromJson(apiResponse.body(), JsonArray.class);
            int responseLength = jsonResponseArray.size();
            System.out.println("=> Api Call Successful -> Received " + responseLength + " Products.");
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Load Raw Data to Json on Local with appended datetime stamp
        System.out.println("=> Loading Data to File Using Java..");
        String fileName = "xtracts/products_data_raw_" + runTimeInfo + ".json";

        try (FileWriter writer = new FileWriter(fileName)) {
            gson.toJson(jsonResponseArray, writer);
            System.out.println("=> Done Writing Data to Json File -> " + fileName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return fileName;
    }

    /**
     * Transforms Data in a Json File to tabular format and writes to Csv
     * @param filePath path to Json File.
     * @return filename/path of csv file
     * @throws IOException if i/o operations fail.
     */
    public String transformData(String filePath) throws IOException {

        // Transforms data to json array and then to csv.
        JsonArray fileJsonArray;
        Reader reader = Files.newBufferedReader(Paths.get(filePath));
        fileJsonArray = gson.fromJson(reader, JsonArray.class);
        System.out.println("=> Now Transforming Data using Gson ..");

        // Parse Json to Array
        List<String[]> data = new ArrayList<>();
        // Add headers to array.
        data.add(new String[]{"id", "title", "description", "category", "rate", "count"});
        for (int i = 0; i < fileJsonArray.size(); i++) {
            JsonObject productObject = fileJsonArray.get(i).getAsJsonObject();
            JsonObject ratingObject = productObject.get("rating").getAsJsonObject();
            // Loop through JsonArray , transform elements and store in List
            data.add(new String[]{
                    Integer.toString(productObject.get("id").getAsInt()),
                    productObject.get("title").getAsString(),
                    productObject.get("description").getAsString(),
                    productObject.get("category").getAsString(),
                    Float.toString(ratingObject.get("rate").getAsFloat()),
                    Integer.toString(ratingObject.get("count").getAsInt()),
            });
        }
        // Write to Csv
        String fileName = "xtracts/products_data_trans_" + runTimeInfo + ".csv";
        try (CSVWriter csvwriter = new CSVWriter(new FileWriter(fileName))) {
            csvwriter.writeAll(data);
            System.out.println("=> Data written to CSV file -> " + fileName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return fileName;
    }

    /**
     * Loads csv data to BigQuery
     * @param datasetName name of existing Bigquery Dataset.
     * @param tableName Name of the Table to load data to (creates or appends to this table).
     * @param sourceUri Uri to csv file on local storage or on GCS/S3.
     */
    public void loadCsvToBigQuery(String datasetName, String tableName, String sourceUri) throws InterruptedException {

        // Initialize client that will be used to send requests to BigQuery. This client only needs to be created
        // once, and can be reused for multiple requests.
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        String location = "US";
        TableId tableId = TableId.of(datasetName, tableName);
        Path csvPath = FileSystems.getDefault().getPath(".", sourceUri);

        WriteChannelConfiguration writeChannelConfiguration =
                WriteChannelConfiguration.newBuilder(tableId)
                        .setFormatOptions(FormatOptions.csv())
                        .setAutodetect(true)
                        .build();

        JobId jobId = JobId.newBuilder().setLocation(location).build();
        TableDataWriteChannel writer = bigquery.writer(jobId, writeChannelConfiguration);
        // Write csv data to writer stream
        try (OutputStream stream = Channels.newOutputStream(writer)) {
            Files.copy(csvPath, stream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // Start load job
        System.out.println("=> Now Loading Data Into BigQuery .....");
        Job job = writer.getJob();
        job = job.waitFor();
        JobStatistics.LoadStatistics stats = job.getStatistics();
        if (job.isDone() && job.getStatus().getError() == null) {

            System.out.println("=> CSV Schema Auto-detected from File and " + stats.getOutputRows() + " Rows loaded to -> " + datasetName + "." + tableName);
        } else {
            System.out.println(
                    "BigQuery was unable to load into the table due to an error:"
                            + job.getStatus().getError());
        }
    }

    public DataTransformer() {

    }
}
