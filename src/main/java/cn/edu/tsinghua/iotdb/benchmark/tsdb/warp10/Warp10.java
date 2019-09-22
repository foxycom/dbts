package cn.edu.tsinghua.iotdb.benchmark.tsdb.warp10;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.Database;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Point;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Bike;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.SensorGroup;
import io.mikael.urlbuilder.UrlBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class Warp10 implements Database {
  private static Config config = ConfigParser.INSTANCE.config();
  private static final Logger LOGGER = LoggerFactory.getLogger(Warp10.class);
  private static String fetchUri = "http://%s:%s/api/v0/fetch";
  private static String writeUri = "http://%s:%s/api/v0/update";
  private static String deleteUri = "http://%s:%s/api/v0/delete";
  private static String execUri = "http://%s:%s/api/v0/exec";

  private HttpClient client;
  private STGroupFile templatesFile;

  public Warp10() {
    client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    fetchUri = String.format(fetchUri, config.HOST, config.PORT);
    writeUri = String.format(writeUri, config.HOST, config.PORT);
    deleteUri = String.format(deleteUri, config.HOST, config.PORT);
    execUri = String.format(execUri, config.HOST, config.PORT);
    templatesFile = new STGroupFile("../templates/warp10/scenarios.stg");
  }

  @Override
  public void init() throws TsdbException {}

  @Override
  public void cleanup() throws TsdbException {
    LOGGER.debug("Erasing data...");
    URI deleteAllUri =
        UrlBuilder.fromString(deleteUri)
            .addParameter("deleteall", "true")
            .addParameter("selector", "~.*{}")
            .toUri();
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(deleteAllUri)
            .header("X-Warp10-Token", config.WRITE_TOKEN)
            .GET()
            .build();
    try {
      client.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (IOException | InterruptedException e) {
      throw new TsdbException(e);
    }
  }

  @Override
  public void close() throws TsdbException {
    // Not needed.
  }

  @Override
  public void registerSchema(List<Bike> schemaList) throws TsdbException {
    // Not needed.
  }

  @Override
  public float getSize() throws TsdbException {
    return 0;
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    String entryTemplate = "%d/%s:%s/ %s{bike_id=%s,sensor_id=%s} %s\n";
    String subsequentEntryTemplate = "=%d/%s:%s/ %s\n";
    StringBuilder sb = new StringBuilder();
    Map<Sensor, Reading[]> readings = transform(batch);
    for (Sensor sensor : readings.keySet()) {
      String className = sensor.getSensorGroup().getName();
      Reading[] sensorReadings = readings.get(sensor);
      boolean firstIteration = true;
      for (Reading reading : sensorReadings) {
        if (firstIteration) {
          sb.append(
              String.format(
                  Locale.US,
                  entryTemplate,
                  trailingZeros(reading.timestamp),
                  reading.gpsLocation.getValues()[0],
                  reading.gpsLocation.getValues()[1],
                  className,
                  batch.getBike().getName(),
                  sensor.getName(),
                  reading.sensorReading.getValue()));
          firstIteration = false;
        } else {
          sb.append(
              String.format(
                  Locale.US,
                  subsequentEntryTemplate,
                  trailingZeros(reading.timestamp),
                  reading.gpsLocation.getValues()[0],
                  reading.gpsLocation.getValues()[1],
                  reading.sensorReading.getValue()));
        }
      }
    }
    return write(sb.toString());
  }

  @Override
  public Status precisePoint(Query query) {
    Sensor sensor = query.getSensor();
    String timestamp = parseTimestamp(query.getStartTimestamp());

    Bike bike = query.getBikes().get(0);
    String selector =
        sensor.getSensorGroup().getName()
            + "{bike_id="
            + bike.getName()
            + ",sensor_id="
            + sensor.getName()
            + "}";
    URI uri =
        UrlBuilder.fromString(fetchUri)
            .addParameter("start", timestamp)
            .addParameter("stop", timestamp)
            .addParameter("selector", selector)
            .addParameter("format", "text")
            .toUri();
    return fetch(uri);
  }

  @Override
  public Status gpsPathScan(Query query) {
    Sensor sensor = query.getSensor();
    SensorGroup sensorGroup = sensor.getSensorGroup();
    Bike bike = query.getBikes().get(0);
    String endTimestamp = parseTimestamp(query.getEndTimestamp());
    String startTimestamp = parseTimestamp(query.getStartTimestamp());
    ST template = templatesFile.getInstanceOf("gpsPath");
    template
        .add("readToken", config.READ_TOKEN)
        .add("class", sensorGroup.getName())
        .add("sensor", sensor.getName())
        .add("bike", bike.getName())
        .add("end", endTimestamp)
        .add("start", startTimestamp);
    String warpScript = template.render();
    return exec(warpScript);
  }

  @Override
  public Status identifyTrips(Query query) {
    Sensor sensor = query.getSensor();
    SensorGroup sensorGroup = sensor.getSensorGroup();
    String endTimestamp = parseTimestamp(query.getEndTimestamp());
    String startTimestamp = parseTimestamp(query.getStartTimestamp());
    Bike bike = query.getBikes().get(0);
    ST template = templatesFile.getInstanceOf("identifyTrips");
    template
        .add("readToken", config.READ_TOKEN)
        .add("class", sensorGroup.getName())
        .add("bike", bike.getName())
        .add("sensor", sensor.getName())
        .add("end", endTimestamp)
        .add("start", startTimestamp)
        .add("threshold", query.getThreshold());
    String warpScript = template.render();
    return exec(warpScript);
  }

  @Override
  public Status offlineBikes(Query query) {
    Sensor sensor = query.getSensor();
    SensorGroup sensorGroup = sensor.getSensorGroup();
    String endTimestamp = parseTimestamp(query.getEndTimestamp());
    String startTimestamp = parseTimestamp(query.getStartTimestamp());
    ST template = templatesFile.getInstanceOf("offlineBikes");
    template
        .add("readToken", config.READ_TOKEN)
        .add("bikesNum", config.DEVICES_NUMBER)
        .add("class", sensorGroup.getName())
        .add("sensor", sensor.getName())
        .add("end", endTimestamp)
        .add("start", startTimestamp);
    String warpScript = template.render();
    return exec(warpScript);
  }

  @Override
  public Status lastTimeActivelyDriven(Query query) {
    Sensor sensor = query.getSensor();
    SensorGroup sensorGroup = sensor.getSensorGroup();
    String endTimestamp = parseTimestamp(query.getEndTimestamp());
    String startTimestamp = parseTimestamp(query.getStartTimestamp());
    ST template = templatesFile.getInstanceOf("lastTimeActivelyDriven");
    template
        .add("readToken", config.READ_TOKEN)
        .add("class", sensorGroup.getName())
        .add("end", endTimestamp)
        .add("start", startTimestamp)
        .add("threshold", query.getThreshold());
    String warpScript = template.render();
    return exec(warpScript);
  }

  @Override
  public Status downsample(Query query) {
    Sensor sensor = query.getSensor();
    SensorGroup sensorGroup = sensor.getSensorGroup();
    String endTimestamp = parseTimestamp(query.getEndTimestamp());
    String startTimestamp = parseTimestamp(query.getStartTimestamp());
    Bike bike = query.getBikes().get(0);
    ST template = templatesFile.getInstanceOf("downsample");
    template
        .add("readToken", config.READ_TOKEN)
        .add("class", sensorGroup.getName())
        .add("sensor", sensor.getName())
        .add("bike", bike.getName())
        .add("end", endTimestamp)
        .add("start", startTimestamp);
    String warpScript = template.render();
    return exec(warpScript);
  }

  @Override
  public Status lastKnownPosition(Query query) {
    Sensor sensor = query.getSensor();
    SensorGroup sensorGroup = sensor.getSensorGroup();
    ST template = templatesFile.getInstanceOf("lastKnownPosition");
    template
        .add("readToken", config.READ_TOKEN)
        .add("class", sensorGroup.getName())
        .add("sensor", sensor.getName());
    String warpScript = template.render();
    return exec(warpScript);
  }

  @Override
  public Status airPollutionHeatMap(Query query) {
    Sensor sensor = query.getSensor();
    SensorGroup sensorGroup = sensor.getSensorGroup();
    String endTimestamp = parseTimestamp(query.getEndTimestamp());
    String startTimestamp = parseTimestamp(query.getStartTimestamp());
    ST template = templatesFile.getInstanceOf("airPollutionHeatMap");
    template
        .add("readToken", config.READ_TOKEN)
        .add("class", sensorGroup.getName())
        .add("sensor", sensor.getName())
        .add("start", startTimestamp)
        .add("end", endTimestamp);
    String warpScript = template.render();
    return exec(warpScript);
  }

  @Override
  public Status distanceDriven(Query query) {
    Sensor sensor = query.getSensor();
    SensorGroup sensorGroup = sensor.getSensorGroup();
    String endTimestamp = parseTimestamp(query.getEndTimestamp());
    String startTimestamp = parseTimestamp(query.getStartTimestamp());
    Bike bike = query.getBikes().get(0);
    ST template = templatesFile.getInstanceOf("distanceDriven");
    template
        .add("readToken", config.READ_TOKEN)
        .add("class", sensorGroup.getName())
        .add("bike", bike.getName())
        .add("sensor", sensor.getName())
        .add("end", endTimestamp)
        .add("start", startTimestamp)
        .add("threshold", query.getThreshold());
    String warpScript = template.render();
    return exec(warpScript);
  }

  @Override
  public Status bikesInLocation(Query query) {
    Sensor sensor = query.getSensor();
    SensorGroup sensorGroup = sensor.getSensorGroup();
    String endTimestamp = parseTimestamp(query.getEndTimestamp());
    String startTimestamp = parseTimestamp(query.getStartTimestamp());
    ST template = templatesFile.getInstanceOf("bikesInLocation");
    template
        .add("readToken", config.READ_TOKEN)
        .add("class", sensorGroup.getName())
        .add("sensor", sensor.getName())
        .add("end", endTimestamp)
        .add("start", startTimestamp);
    String warpScript = template.render();
    return exec(warpScript);
  }

  private long trailingZeros(long ts) {
    return ts * 1000;
  }

  private String parseTimestamp(long ts) {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    return df.format(new Date(ts));
  }

  private Map<Sensor, Reading[]> transform(Batch batch) {
    Map<Sensor, Point[]> entries = batch.getEntries();
    Sensor gpsSensor = null;
    for (Sensor sensor : entries.keySet()) {
      if (sensor.getSensorGroup().getName().contains("gps")) {
        gpsSensor = sensor;
        break;
      }
    }

    Point[] gpsEntries = entries.get(gpsSensor);
    Map<Long, Point> gpsLookupTable = new HashMap<>(gpsEntries.length);
    Arrays.stream(gpsEntries)
        .forEachOrdered(
            point -> {
              gpsLookupTable.put(point.getTimestamp(), point);
            });

    Map<Sensor, Reading[]> readings = new HashMap<>(gpsEntries.length);
    for (Sensor sensor : entries.keySet()) {
      if (sensor.equals(gpsSensor)) {
        continue;
      }

      Point[] points = entries.get(sensor);
      Reading[] sensorReadings = new Reading[points.length];
      for (int i = 0; i < points.length; i++) {
        Point point = points[i];
        sensorReadings[i] =
            new Reading(point.getTimestamp(), point, gpsLookupTable.get(point.getTimestamp()));
      }
      readings.put(sensor, sensorReadings);
    }
    return readings;
  }

  private Status exec(String warpScript) {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(execUri))
            .timeout(Duration.ofMinutes(10))
            .header("Content-Type", "text/plain")
            .POST(HttpRequest.BodyPublishers.ofString(warpScript))
            .build();
    return send(request);
  }

  private Status fetch(URI uri) {
    LOGGER.debug("{} fetches data", Thread.currentThread().getName());
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(uri)
            .timeout(Duration.ofMinutes(10))
            .header("Content-Type", "text/plain")
            .header("X-Warp10-Token", config.READ_TOKEN)
            .GET()
            .build();

    return send(request);
  }

  private Status write(String body) {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(writeUri))
            .timeout(Duration.ofMinutes(10))
            .header("Content-Type", "text/plain")
            .header("X-Warp10-Token", config.WRITE_TOKEN)
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

    return send(request);
  }

  private Status send(HttpRequest request) {
    HttpResponse<String> res;
    long startTimestamp = System.nanoTime();
    try {
      res = client.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (IOException | InterruptedException e) {
      return new Status(false, 0, e, e.getMessage());
    }
    if (res.statusCode() != 200) {
      LOGGER.debug(
          "Could not process request with code {} because {}", res.statusCode(), res.body());
      return new Status(false, 0, new TsdbException(), res.body());
    }
    long endTimestamp = System.nanoTime();
    Map<String, List<String>> headers = res.headers().map();
    int fetchedPoints = 0;
    if (headers.get("x-warp10-fetched") != null) {
      fetchedPoints = Integer.parseInt(headers.get("x-warp10-fetched").get(0));
    }
    return new Status(true, endTimestamp - startTimestamp, fetchedPoints);
  }

  private class Reading {
    long timestamp;
    Point sensorReading;
    Point gpsLocation;

    public Reading(long timestamp, Point sensorReading, Point gpsLocation) {
      this.sensorReading = sensorReading;
      this.gpsLocation = gpsLocation;
      this.timestamp = timestamp;
    }
  }
}
