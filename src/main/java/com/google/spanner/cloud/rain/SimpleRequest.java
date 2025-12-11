package com.google.spanner.cloud.rain;

import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.grpc.fallback.GcpFallbackChannel;
import com.google.cloud.grpc.fallback.GcpFallbackChannelOptions;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.core.GaxProperties;
import com.google.api.gax.rpc.ApiClientHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.api.gax.grpc.GrpcTransportChannel;
import io.grpc.auth.MoreCallCredentials;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.grpc.fallback.GcpFallbackChannel;
import com.google.cloud.grpc.fallback.GcpFallbackChannelOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.api.gax.grpc.GrpcTransportChannel;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import com.google.cloud.ServiceOptions;


import io.grpc.Grpc;
import io.grpc.alts.AltsChannelCredentials;
import io.grpc.ChannelCredentials;
import io.grpc.CallCredentials;
import io.grpc.alts.GoogleDefaultChannelCredentials;
import com.google.auth.oauth2.ComputeEngineCredentials;


import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

// Add these imports to your file for the metrics configuration:
import com.google.cloud.grpc.fallback.GcpFallbackOpenTelemetry;
import java.util.Collections;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import java.time.Duration;
import com.google.cloud.opentelemetry.metric.GoogleCloudMetricExporter;
import io.opentelemetry.sdk.metrics.export.MetricExporter;

import java.util.logging.LogManager;
import java.io.InputStream;

/** */
public final class SimpleRequest {
  private static final Logger logger = Logger.getLogger(SimpleRequest.class.getName());
  private static ManagedChannel eefChannel = null; // Declare here for access in finally

  private SimpleRequest() {}

  public static void createTableIfNotExists(
      DatabaseAdminClient dbAdminClient, String instanceId, String databaseId, int numCols)
      throws Exception {
    StringBuilder ddl = new StringBuilder("CREATE TABLE LargeColumns ( Key INT64 NOT NULL");
    for (int i = 0; i <= numCols; i++) {
      ddl.append(", Col" + i + " STRING(MAX)");
    }
    ddl.append(") PRIMARY KEY (Key)");
    List<String> statements = new ArrayList<>();
    statements.add(ddl.toString());
    try {
      dbAdminClient.updateDatabaseDdl(instanceId, databaseId, statements, null).get();
      System.out.println("Created LargeColumns table.");
    } catch (ExecutionException e) {
      if (e.getCause() != null && e.getCause().getMessage().contains("Duplicate name in schema")) {
        System.out.println("LargeColumns table already exists.");
      }
    }
  }

  public static void insertOrUpdateRandomBytes(DatabaseClient dbClient, int i) {
    Random random = new Random();

    Mutation mutation =
        Mutation.newInsertOrUpdateBuilder("LargeColumns")
            .set("Key")
            .to(i)
            .set("Col0")
            .to("test_string 9/8 18:05")
            .build();
    try {
      dbClient.write(Arrays.asList(mutation));
      logger.log(Level.INFO, "Finished writing random bytes to key " + Integer.toString(i));
    } catch (Exception e) {
      logger.log(Level.INFO, "Exception during insertOrUpdateRandomBytes: " + e.getCause().getMessage());
    }
  }



  public static CallCredentials createHardBoundTokensCallCredentials(
	ComputeEngineCredentials credentials,
        ComputeEngineCredentials.GoogleAuthTransport googleAuthTransport,
        ComputeEngineCredentials.BindingEnforcement bindingEnforcement) {
      ComputeEngineCredentials.Builder credsBuilder =
          ((ComputeEngineCredentials) credentials).toBuilder();
      // We only set scopes and HTTP transport factory from the original credentials because
      // only those are used in gRPC CallCredentials to fetch request metadata. We create a new
      // credential
      // via {@code newBuilder} as opposed to {@code toBuilder} because we don't want a reference to
      // the
      // access token held by {@code credentials}; we want this new credential to fetch a new access
      // token
      // from MDS using the {@param googleAuthTransport} and {@param bindingEnforcement}.
      return MoreCallCredentials.from(
          ComputeEngineCredentials.newBuilder()
              .setScopes(credsBuilder.getScopes())
              .setHttpTransportFactory(credsBuilder.getHttpTransportFactory())
              .setGoogleAuthTransport(googleAuthTransport)
              .setBindingEnforcement(bindingEnforcement)
              .build());
    }


/**
  * Builds the SpannerOptions, configuring the GcpFallbackChannel if enabled.
  */
public static SpannerOptions buildSpannerOptions(String projectId, boolean fallbackEnabled) throws IOException {
    SpannerOptions.Builder spannerOptionsBuilder = SpannerOptions.newBuilder().setProjectId(projectId);
    System.out.println("buildSpannerOptions projectId: " + projectId);
    // --- METRICS CONFIGURATION START ---

    // 1. Set up an exporter. For production, you would use a Google Cloud Monitoring exporter.
    // For this example, we use a simple in-memory exporter.
    MetricExporter exporter = GoogleCloudMetricExporter.createWithDefaultConfiguration();
    SdkMeterProvider meterProvider = SdkMeterProvider.builder()
        .registerMetricReader(
            PeriodicMetricReader.builder(exporter).setInterval(Duration.ofSeconds(10)).build())
        .build();
    OpenTelemetry openTelemetry = OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build();

    // 2. Create the GcpFallbackOpenTelemetry object with the SDK.
    GcpFallbackOpenTelemetry fallbackTelemetry = GcpFallbackOpenTelemetry.newBuilder()
        .withSdk(openTelemetry)
        .build();

    // --- METRICS CONFIGURATION END ---

    if (fallbackEnabled) {
      // Create builders for both paths.
      String targetEndpoint = "spanner.googleapis.com:443";
      String dpTargetEndpoint = "google-c2p:///spanner.googleapis.com";

      ChannelCredentials credentials = AltsChannelCredentials.create();


      CallCredentials altsCallCredentials =
            createHardBoundTokensCallCredentials(
                ComputeEngineCredentials.create(),
		ComputeEngineCredentials.GoogleAuthTransport.ALTS, null);
      System.out.println("altsCallCredentials: " + altsCallCredentials);

      ChannelCredentials channelCreds =
          GoogleDefaultChannelCredentials.newBuilder()
              .altsCallCredentials(altsCallCredentials)
              .build();



      ManagedChannelBuilder<?> dpBuilder = Grpc.newChannelBuilder(dpTargetEndpoint, channelCreds);
      ManagedChannelBuilder<?> cpBuilder = ManagedChannelBuilder.forTarget(targetEndpoint);


      GcpFallbackChannelOptions.Builder eefOptionsBuilder = GcpFallbackChannelOptions.newBuilder()
          .setPrimaryChannelName("directpath")
          .setFallbackChannelName("cloudpath")
          .setErrorRateThreshold(0.1f)
          .setPeriod(Duration.ofSeconds(10))
          .setMinFailedCalls(1);

      eefOptionsBuilder.setGcpFallbackOpenTelemetry(fallbackTelemetry);

      GcpFallbackChannelOptions eefOptions = eefOptionsBuilder.build();

      eefChannel = new GcpFallbackChannel(eefOptions, cpBuilder, cpBuilder);
      logger.log(Level.INFO, "GcpFallbackChannel enabled with metrics.");

      TransportChannelProvider channelProvider = FixedTransportChannelProvider.create(
          GrpcTransportChannel.create(eefChannel));
      spannerOptionsBuilder.setChannelProvider(channelProvider);
    } else {
      logger.log(Level.INFO, "GcpFallbackChannel disabled. Using default channel provider. Metrics will not be exported.");
    }

    return spannerOptionsBuilder.build();
}

  public static void main(String[] args) throws InterruptedException, IOException {
//     try {
//     InputStream configFile = SimpleRequest.class.getClassLoader().getResourceAsStream("logging.properties");
//     if (configFile != null) {
//         LogManager.getLogManager().readConfiguration(configFile);
//         System.out.println("Successfully loaded logging configuration from logging.properties");
//     } else {
//         System.err.println("CRITICAL: Could not find logging.properties in classpath!");
//     }
// } catch (Exception e) {
//     System.err.println("CRITICAL: Error loading logging configuration: " + e.getMessage());
//     e.printStackTrace();
// }

    int numRequests = 1;
    boolean fallbackEnabled = false;
    for (String arg : args) {
      if (arg.startsWith("num-requests=")) {
        numRequests = Integer.parseInt(arg.substring(arg.indexOf("=") + 1));
      }
      if ("fallback-enabled".equals(arg)) {
        fallbackEnabled = true;
      }
    }

    // Hardcoded values to be replaced by the user
    String projectId1 = "cloud-spanner-perf-testing";
    String projectId2 = "span-cloud-testing";
    String projectId = projectId2;
    String instanceId = "kinsaurralde-test";
    String databaseId = "rain";
    int bytesPerCol = 1024;
    logger.log(
        Level.INFO,
        "Startinggg SimpleRequest with projectId: "
            + projectId
            + " instanceId: "
            + instanceId
            + " databaseId: "
            + databaseId
            + " bytesPerCol: "
            + bytesPerCol);

    SpannerOptions spannerOptions = buildSpannerOptions(projectId, fallbackEnabled);

    logger.log(Level.INFO, "Using credentials: " + spannerOptions.getCredentials());
    Spanner spanner = spannerOptions.getService();
    logger.log(Level.INFO, "Is direct access enabled: " + spannerOptions.isEnableDirectAccess());
    logger.log(Level.INFO, "Spanner Endpoint: " + spannerOptions.getEndpoint());

    try {
      DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
      DatabaseClient dbClient =
          spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));

        // createTableIfNotExists(dbAdminClient, instanceId, databaseId, 1);
      for (int i = 0; i < numRequests; i++) {
        insertOrUpdateRandomBytes(dbClient, i);
        Thread.sleep(1000);
      }

    } catch (Exception e) {
      logger.log(Level.SEVERE, "An error occurred: " + e.getMessage(), e);
    } finally {
      spanner.close();
      if (eefChannel != null) {
        eefChannel.shutdown();
        try {
          eefChannel.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          logger.log(Level.WARNING, "Failed to shut down gRPC channel cleanly.", e);
        }
      }
    }
    logger.log(Level.INFO, "Finished SimpleRequest");
  }
}
