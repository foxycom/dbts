package cn.edu.tsinghua.iotdb.benchmark;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class Test {

    private static String createUri = String.format("http://%s:%s/api/v0/update", "127.0.0.1", "8080");
    private static String readUri = String.format("http://%s:%s/api/v0/fetch", "127.0.0.1", "8080");

    private static HttpClient client;

    public static void main(String[] args) throws IOException, InterruptedException {
        client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        //create();
        read();
    }

    public static void create() throws IOException, InterruptedException {

    }

    public static void read() throws IOException, InterruptedException {
        String fetchUri = readUri + "?start=2000-05-30T13%3A30%3A00.000Z&stop=2000-06-02T00%3A00%3A00.000Z" +
                "&selector=~VehicleSpeed.*%7B%7D&format=text";
        System.out.println(URI.create(fetchUri));
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(fetchUri))
                .timeout(Duration.ofMinutes(1))
                .header("Content-Type", "text/plain")
                .header("X-Warp10-Token", ".Ixgr2xpo4cCUjLrJLfloeoMZ56LXkyzXaaCC1txorCjoDhqwrbEnyjkpBAZlxHm_vzocmHR2zsh0fTaLxvHPzkEzur4mQ.uyg39HaabYOlclh0Qdp2BCV")
                .GET()
                .build();
        HttpResponse<String> res = client.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println(res.body());
    }
}
