package org.apache.beam.sdk.io.pasca;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PascaHttpIO supports write byte array data to kafka topic by pasca. In order to use this IO, Must
 * specify HttpConfiguration, this need http server address and topic name.
 *
 * <pre>{@code
 * PCollection<byte[]> byteColl = ...;
 *
 *    byteColl.apply(PascaHttpIO.write()
 *      .withHttpConfiguration(
 *          PascaHttpIO.HttpConfiguration.create(new String[]{"http://ip:port"},"")
 *      .withUsername("")
 *      .withPassword("))).
 *
 * }</pre>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class PascaHttpIO {

  private static final Logger LOGGER = LoggerFactory.getLogger(PascaHttpIO.class);

  public static Write write() {
    return new AutoValue_PascaHttpIO_Write.Builder().setMaxBatchSizeBytes(5 * 1024 * 1024).build();
  }

  @AutoValue
  public abstract static class HttpConfiguration implements Serializable {

    public abstract List<String> getAddresses();

    public abstract String getTopic();

    @Nullable
    public abstract String getUsername();

    @Nullable
    public abstract String getPassword();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setAddresses(List<String> addresses);

      abstract Builder setTopic(String topic);

      abstract Builder setUsername(String username);

      abstract Builder setPassword(String password);

      abstract HttpConfiguration build();
    }

    public static HttpConfiguration create(String[] addresses, String topic) {
      checkArgument(addresses != null, "addresses can not be null");
      checkArgument(addresses.length > 0, "addresses can not be empty");
      checkArgument(topic != null, "topic can not be null");
      return new AutoValue_PascaHttpIO_HttpConfiguration.Builder()
              .setAddresses(Arrays.asList(addresses))
              .setTopic(topic)
              .build();
    }

    public HttpConfiguration withUsername(String username) {
      checkArgument(username != null, "user can not be null");
      return builder().setUsername(username).build();
    }

    public HttpConfiguration withPassword(String password) {
      checkArgument(password != null, "password can not be null");
      return builder().setPassword(password).build();
    }
  }

  @AutoValue
  public abstract static class Write extends PTransform<PCollection<byte[]>, PDone> {

    @Nullable
    abstract HttpConfiguration getHttpConfiguration();

    abstract int getMaxBatchSizeBytes();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setHttpConfiguration(HttpConfiguration httpConfiguration);

      abstract Builder setMaxBatchSizeBytes(int maxBatchSizeBytes);

      abstract Write build();
    }

    public Write withHttpConfiguration(HttpConfiguration httpConfiguration) {
      checkArgument(null != httpConfiguration, "Http configuration can not be null.");
      return builder().setHttpConfiguration(httpConfiguration).build();
    }

    public Write withMaxBatchSizeBytes(int maxBatchSizeBytes) {
      checkArgument(maxBatchSizeBytes > 0, "Max batch size bytes greater than zero.");
      return builder().setMaxBatchSizeBytes(maxBatchSizeBytes).build();
    }

    @Override
    public PDone expand(PCollection<byte[]> input) {
      HttpConfiguration httpConfiguration = getHttpConfiguration();
      checkArgument(null != httpConfiguration, "http configuration is required.");
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    static class WriteFn extends DoFn<byte[], Void> {
      private transient CloseableHttpClient httpClient;
      private int currentBatchSizeBytes;
      private Write spec;

      private byte[] allBytes;

      @VisibleForTesting
      WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setUp() {
        httpClient = HttpClients.createDefault();
      }

      @StartBundle
      public void startBundle(StartBundleContext context) {
        allBytes = new byte[0];
        currentBatchSizeBytes = 0;
      }

      @ProcessElement
      public void process(ProcessContext context) {
        byte[] element = context.element();

        byte[] temp = allBytes;
        allBytes = new byte[currentBatchSizeBytes + element.length];
        System.arraycopy(temp, 0, allBytes, 0, currentBatchSizeBytes);
        System.arraycopy(element, 0, allBytes, currentBatchSizeBytes, element.length);

        if (currentBatchSizeBytes >= spec.getMaxBatchSizeBytes()) {
          flushBatch();
          allBytes = new byte[0];
        }
        currentBatchSizeBytes = allBytes.length;
      }

      @FinishBundle
      public void finishBundle(FinishBundleContext context) {
        flushBatch();
      }

      private void flushBatch() {
        List<String> addresses = Objects.requireNonNull(spec.getHttpConfiguration()).getAddresses();
        String url = addresses.get((int) (Math.random() * 100) % addresses.size());
        HttpPost httpPost = new HttpPost(url);
        httpPost.addHeader("Content-Type", "binary/octet_stream");
        httpPost.addHeader("Topic", spec.getHttpConfiguration().getTopic());
        httpPost.addHeader("Format", "avro");
        httpPost.addHeader("User", spec.getHttpConfiguration().getUsername());
        httpPost.addHeader("Password", spec.getHttpConfiguration().getPassword());
        if (allBytes.length > 0) {
          ByteArrayEntity entity = new ByteArrayEntity(allBytes);
          httpPost.setEntity(entity);
          try {
            HttpResponse response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            LOGGER.debug(
                    "Response status:{},info:{},topic:{},byte length:{}",
                    statusCode,
                    EntityUtils.toString(response.getEntity()),
                    spec.getHttpConfiguration().getTopic(),
                    allBytes.length);
          } catch (IOException e) {
            LOGGER.error("Exception:", e);
          }
        }
      }
    }
  }
}
