package dk.ku.di.dms.vms.tpcc.proxy.infra;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public final class MinimalHttpClient {

    private final URL url;

    public MinimalHttpClient(String baseUrl) throws IOException {
        this.url = new URL(baseUrl);
    }

    private HttpURLConnection createConnection() throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true); // Enable output stream to send request body
        connection.setUseCaches(false); // Prevent caching
        connection.setRequestProperty("Connection", "keep-alive"); // Keep connection open for reuse
        connection.setDefaultUseCaches(false);
        return connection;
    }

    public int sendRequest(String jsonBody) throws IOException {
        HttpURLConnection connection = this.createConnection();
        try (OutputStream os = connection.getOutputStream()) {
            byte[] input = jsonBody.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }
        return connection.getResponseCode();
        /*
        try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            return response.toString();
        }
         */
    }

}
