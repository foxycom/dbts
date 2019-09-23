package de.uni_passau.dbts.benchmark.tsdb.warp10;

import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import de.uni_passau.dbts.benchmark.measurement.Status;
import de.uni_passau.dbts.benchmark.tsdb.Database;
import de.uni_passau.dbts.benchmark.tsdb.TsdbException;
import de.uni_passau.dbts.benchmark.workload.ingestion.Batch;
import de.uni_passau.dbts.benchmark.workload.ingestion.Point;
import de.uni_passau.dbts.benchmark.workload.query.impl.Query;
import de.uni_passau.dbts.benchmark.workload.schema.Bike;
import de.uni_passau.dbts.benchmark.workload.schema.Sensor;
import de.uni_passau.dbts.benchmark.workload.schema.SensorGroup;
import io.mikael.urlbuilder.UrlBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;

public class Warp10 implements Database {
  private static final Logger LOGGER = LoggerFactory.getLogger(Warp10.class);
  private static Config config = ConfigParser.INSTANCE.config();
  /** API endpoint to fetch data from Warp10. */
  private static String fetchUri = "http://%s:%s/api/v0/fetch";

  /** API endpoint to update / insert data into Warp10 instance. */
  private static String writeUri = "http://%s:%s/api/v0/update";

  /** API endpoint to erase data from Warp10. */
  private static String deleteUri = "http://%s:%s/api/v0/delete";

  /** API endpoint to execute WarpScript. */
  private static String execUri = "http://%s:%s/api/v0/exec";

  /** The client that is used to access HTTP API endpoints of Warp10. */
  private HttpClient client;

  /** WarpScript templates file. */
  private STGroupFile templatesFile;

  /** Creates an instance of Warp10 controller. */
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
      LOGGER.info("Waiting {}ms until old data has been erased.", config.ERASE_WAIT_TIME);
      Thread.sleep(config.ERASE_WAIT_TIME);
    } catch (IOException | InterruptedException e) {
      throw new TsdbException(e);
    }
  }

  @Override
  public void close() throws TsdbException {
    // Not needed in Warp10 since there is no persistent connection to the DBMS.
  }

  @Override
  public void registerSchema(List<Bike> schemaList) throws TsdbException {
    // Not needed in Warp10 since it's schema-less.
  }

  @Override
  public float getSize() throws TsdbException {
    return 0;
  }

  /**
   * {@inheritDoc} The whole batch is placed into the body of a HTTP request in the format:
   *
   * <p><code>
   * 1535587200000000/48.544948:13.434205/ force{bike_id=bike_9,sensor_id=s_11} 1493.67 <br>
   * =1535587200020000/48.544948:13.434205/ 1950.35 <br>
   * =1535587200040000/48.544948:13.434205/ 1010.31 <br>
   * =1535587200060000/48.544948:13.434205/ 1040.96
   * </code>
   *
   * <p>Warp10 divides time series into classes, each having a set of labels that are somewhat like
   * InfluxDB's tags. Each entry must belong to a class and might have a set of labels. Here, each
   * sensor group is stored as a separate class with labels being <code>{bike_id, sensor_id}</code>.
   * An entry has the following structure:
   *
   * <p><code>[time]/[latitude]:[longitude]/[elevation] [class]{[labels]} [value]</code>
   *
   * <p>In case an entry is preceded with [=], previous class name and labels set are applied.
   *
   * <p>See <a
   * href="https://www.warp10.io/content/03_Documentation/03_Interacting_with_Warp_10/03_Ingesting_data/02_GTS_input_format">Warp10
   * GTS input format</a> for more.
   */
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

  /**
   * {@inheritDoc} Example URL:
   *
   * <p><code>
   * http://127.0.0.1:8080/api/v0/fetch?start=2018-08-30T00:00:00.000Z&amp;stop=2018-08-30T00:00:00.000Z&amp;selector=light{bike_id=bike_8,sensor_id=s_33}&amp;format=text
   * </code>
   */
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

  /**
   * {@inheritDoc} Example WarpScript body:
   *
   * <p><code>
   * 'some token' <br>
   * 'read_token' STORE <br>
   * <p>
   * [ <br>
   * $read_token <br>
   * 'current' <br>
   * { 'bike_id' 'bike_8' 'sensor_id' 's_17' } <br>
   * '2018-08-30T01:00:00.000Z' <br>
   * '2018-08-30T00:00:00.000Z' <br>
   * ] FETCH <br>
   * <p>
   * 0 GET 'gts' STORE <br>
   * $gts TICKS <br>
   * $gts LOCATIONS <br>
   * </code>
   */
  @Override
  public Status gpsPathScan(Query query) {
    Sensor sensor = query.getSensor();
    SensorGroup sensorGroup = sensor.getSensorGroup();
    Bike bike = query.getBikes().get(0);
    String endTimestamp = parseTimestamp(query.getEndTimestamp());
    String startTimestamp = parseTimestamp(query.getStartTimestamp());
    ST template = templatesFile.getInstanceOf("gpsPathScan");
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

  /**
   * {@inheritDoc} Example WarpScript body:
   *
   * <p><code>
   * 'some token' <br>
   * 'read_token' STORE <br></code>
   *
   * <p>// Fetches data within a time range:
   *
   * <p><code>
   * [ <br>
   * $read_token <br>
   * 'current' <br>
   * { 'bike_id' 'bike_5' 'sensor_id' 's_17' } <br>
   * '2018-08-30T01:00:00.000Z' <br>
   * '2018-08-30T00:00:00.000Z' <br>
   * ] FETCH <br></code>
   *
   * <p>// Downsample to 1 second buckets:
   *
   * <p><code>
   * [ <br>
   * SWAP <br>
   * bucketizer.mean <br>
   * 0 1 s 0 <br>
   * ] BUCKETIZE <br></code>
   *
   * <p>// Selects buckets with values greater than 1000:
   *
   * <p><code>
   * [ <br>
   * SWAP <br>
   * 1000.0 mapper.gt <br>
   * 0 0 0 <br>
   * ] MAP <br>
   * </code>
   */
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

  /**
   * {@inheritDoc} Example WarpScript body:
   *
   * <p><code>
   * 'some token' <br>
   * 'read_token' STORE <br></code>
   *
   * <p>// Generates a list of all bikes:
   *
   * <p><code>
   * [] 'bikes' STORE <br>
   * 0 12 1 - <br>
   * &lt;% <br>
   * 'i' STORE <br>
   * $bikes [ 'bike_' $i TOSTRING + ] APPEND DROP <br>
   * %&gt; <br>
   * FOR <br>
   * [] 'offlineBikes' STORE <br></code>
   *
   * <p>// Fetches accelerometer readings within the time range:
   *
   * <p><code>
   * [ <br>
   * $read_token <br>
   * 'accelerometer' <br>
   * { 'sensor_id' 's_0' } <br>
   * '2018-08-30T01:00:00.000Z' <br>
   * '2018-08-30T00:00:00.000Z' <br>
   * ] FETCH NONEMPTY <br></code>
   *
   * <p>// Gets bike ids from the fetched data:
   *
   * <p><code>
   * &lt;% <br>
   * DROP <br>
   * LABELS 'bike_id' GET <br>
   * %&gt; <br>
   * LMAP 'gtsBikes' STORE <br></code>
   *
   * <p>// Compares all bike ids with fetched bike ids and stores those not included in the fetched
   * data:
   *
   * <p><code>
   * $bikes SIZE 1 - 'size' STORE <br>
   * 0 $size <br>
   * &lt;% <br>
   * 'i' STORE <br>
   * $bikes $i GET 'bike_id' STORE <br>
   * &lt;% $gtsBikes $bike_id CONTAINS SWAP DROP %&gt; <br>
   * &lt;%  %&gt; <br>
   * &lt;% $offlineBikes $bike_id +! DROP %&gt; <br>
   * IFTE <br>
   * %&gt; FOR <br>
   * $offlineBikes <br>
   * </code>
   */
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

  /**
   * {@inheritDoc} Example WarpScript body:
   *
   * <p><code>
   * 'some token' <br>
   * 'read_token' STORE <br></code>
   *
   * <p>// Fetches data within time range:
   *
   * <p><code>
   * [ <br>
   * [  <br>
   * [ <br>
   * $read_token <br>
   * 'current' <br>
   * {} <br>
   * '2018-08-30T01:00:00.000Z' <br>
   * '2018-08-30T00:00:00.000Z' <br>
   * ] FETCH <br></code>
   *
   * <p>// Downsamples to 1 second buckets:
   *
   * <p><code>
   * bucketizer.mean <br>
   * 0 1 s 0 <br>
   * ] BUCKETIZE NONEMPTY <br></code>
   *
   * <p>// Selects only buckets with values greater than 1000:
   *
   * <p><code>
   * 1000.0 <br>
   * mapper.gt <br>
   * 0 0 0 <br>
   * ] MAP RSORT <br></code>
   *
   * <p>// Selects last timestamp for each bike_id:
   *
   * <p><code>
   * <p>
   * &lt;% <br>
   * DROP 'gts' STORE <br>
   * [] 'list' STORE <br>
   * $list $gts LABELS 'bike_id' GET +! DROP <br>
   * $list $gts LASTTICK +! DROP <br>
   * $list <br>
   * %&gt; <br>
   * LMAP <br>
   * </code>
   */
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

  /**
   * {@inheritDoc} Example WarpScript body:
   *
   * <p><code>
   * 'some token' <br>
   * 'read_token' STORE <br></code>
   *
   * <p>// Fetches data within a time range:
   *
   * <p><code>
   * [ <br>
   * $read_token <br>
   * 'oximeter' <br>
   * { 'bike_id' 'bike_10' 'sensor_id' 's_27' } <br>
   * '2018-08-30T01:00:00.000Z' <br>
   * '2018-08-30T00:00:00.000Z' <br>
   * ] FETCH <br> </code>
   *
   * <p>// Downsamples data to 1 minute buckets:
   *
   * <p><code>
   * [ <br>
   * SWAP <br>
   * bucketizer.mean <br>
   * 0 1 m 0 <br>
   * ] BUCKETIZE <br>
   * </code>
   */
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

  /**
   * {@inheritDoc} Example WarpScript body:
   *
   * <p><code>
   * 'some token' <br>
   * 'read_token' STORE <br></code>
   *
   * <p>// Fetches GPS data of all bikes:
   *
   * <p><code>
   * [ <br>
   * $read_token <br>
   * 'gps' <br>
   * { 'sensor_id' 's_12' } <br>
   * MAXLONG <br>
   * MINLONG <br>
   * ] FETCH <br></code>
   *
   * <p>// Selects last value for each bike:
   *
   * <p><code>
   * [ <br>
   * SWAP <br>
   * mapper.last <br>
   * MAXLONG MAXLONG 1 <br>
   * ] MAP <br>
   * </code>
   */
  @Override
  public Status lastKnownPosition(Query query) {
    Sensor gpsSensor = query.getGpsSensor();
    SensorGroup sensorGroup = gpsSensor.getSensorGroup();
    ST template = templatesFile.getInstanceOf("lastKnownPosition");
    template
        .add("readToken", config.READ_TOKEN)
        .add("class", sensorGroup.getName())
        .add("sensor", gpsSensor.getName());
    String warpScript = template.render();
    return exec(warpScript);
  }

  /**
   * {@inheritDoc} Example WarpScript body:
   *
   * <p><code>
   * 'some token' <br>
   * 'read_token' STORE <br> </code>
   *
   * <p>// Fetches values of particles sensors within a time range:
   *
   * <p><code>
   * [ <br>
   * $read_token <br>
   * 'particles' <br>
   * { 'sensor_id' 's_34' } <br>
   * '2018-08-30T01:00:00.000Z' <br>
   * '2018-08-30T00:00:00.000Z' <br>
   * ] FETCH <br></code>
   *
   * <p>// Downsamples to 1 second buckets:
   *
   * <p><code>
   * &lt;% <br>
   * DROP 'gts' STORE <br>
   * [ <br>
   * $gts <br>
   * bucketizer.mean <br>
   * 0 1 s 0 <br>
   * ] <br>
   * BUCKETIZE NONEMPTY <br>
   * <p>
   * %&gt; LMAP FLATTEN <br>
   * <p>
   * [] 'list' STORE <br></code>
   *
   * <p>// Selects distinct coordinates and respective particles values:
   *
   * <p><code>
   * &lt;%
   * SWAP 'gts' STORE <br>
   * $gts LOCATIONS <br>
   * 'latitudes' STORE <br>
   * 'longitudes' STORE <br>
   * $gts VALUES 'values' STORE <br>
   * 0 $values SIZE 1 - <br>
   * &lt;% <br>
   * 'i' STORE <br>
   * $list [ $values $i GET $latitudes $i GET $longitudes $i GET ] +! <br>
   * %&gt; <br>
   * FOR <br>
   * %&gt; <br>
   * LFLATMAP
   * </code>
   */
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

  /**
   * {@inheritDoc} Example WarpScript body:
   *
   * <p><code>
   * 'some token' <br>
   * 'read_token' STORE <br></code>
   *
   * <p>// Fetches current sensor data of a bike within a time range:
   *
   * <p><code>
   * [ <br>
   * $read_token <br>
   * 'current' <br>
   * { 'bike_id' 'bike_8' 'sensor_id' 's_17' } <br>
   * '2018-08-30T01:00:00.000Z' <br>
   * '2018-08-30T00:00:00.000Z' <br>
   * ] FETCH <br></code>
   *
   * <p>// Downsamples data to 1 second buckets:
   *
   * <p><code>
   * [
   * SWAP <br>
   * bucketizer.mean <br>
   * 0 1 s 0 <br>
   * ] BUCKETIZE <br></code>
   *
   * <p>// Selects buckets with values greater than 1000:
   *
   * <p><code>
   * [
   * SWAP <br>
   * 1000.0 mapper.gt <br>
   * 0 0 0 <br>
   * ] MAP <br></code>
   *
   * <p>// Computes overall distance:
   *
   * <p><code>
   * [ <br>
   * SWAP <br>
   * mapper.hdist  <br>
   * MAXLONG <br>
   * MAXLONG <br>
   * 1 <br>
   * ] MAP <br>
   * </code>
   */
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

  /**
   * {@inheritDoc} Example WarpScript body:
   *
   * <p><code>
   * 'some token' <br>
   * 'read_token' STORE <br> </code>
   *
   * <p>// Stores area boundaries:
   *
   * <p><code>
   * 'POLYGON ((13.4406567 48.5723195, 13.4373522 48.5707861, 13.4373522 48.5662708, <br>
   * 13.4443045 48.5645384, 13.4489393 48.5683155, 13.4492826 48.5710701, 13.4406567  <br>
   * 48.5723195))' 0.1 false GEO.WKT 'area' STORE <br></code>
   *
   * <p>// Fetches data within a time range:
   *
   * <p><code>
   * [ <br>
   * $read_token <br>
   * 'emg' <br>
   * { 'sensor_id' 's_40' } <br>
   * '2018-08-30T01:00:00.000Z' <br>
   * '2018-08-30T00:00:00.000Z' <br>
   * ] FETCH <br></code>
   *
   * <p>// Selects last values for each bike:
   *
   * <p><code>
   * [ <br>
   * SWAP <br>
   * mapper.last <br>
   * MAXLONG MAXLONG 1 <br>
   * ] MAP <br></code>
   *
   * <p>// Checks if last coordinates lie within the area boundaries:
   *
   * <p><code>
   * [ <br>
   * SWAP <br>
   * $area mapper.geo.within <br>
   * 0 0 0 <br>
   * ] MAP NONEMPTY <br>
   * </code>
   */
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
            .header("Content-Encoding", "gzip")
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
      LOGGER.debug("An error occurred while sending request: ", e);
      return new Status(false, 0, e, e.getMessage());
    }
    if (res.statusCode() != 200) {
      LOGGER.error(
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
