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

/** */
public final class SimpleRequest {
  private static final Logger logger = Logger.getLogger(SimpleRequest.class.getName());

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
    dbClient.write(Arrays.asList(mutation));
    logger.log(Level.INFO, "Finished writing random bytes to key 0");
  }

  public static void main(String[] args) throws InterruptedException {
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
    String projectId = "cloud-spanner-perf-testing";
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

    logger.log(Level.INFO, "Classloader for SimpleRequest: " + SimpleRequest.class.getClassLoader());
    logger.log(
        Level.INFO,
        "Classloader for NameResolverProvider: "
            + io.grpc.NameResolverProvider.class.getClassLoader());
    logger.log(
        Level.INFO, "Classloader for Message: " + com.google.protobuf.Message.class.getClassLoader());

    String targetEndpoint = "spanner.googleapis.com:443";
    ManagedChannelBuilder<?> dpBuilder = ManagedChannelBuilder.forTarget(targetEndpoint);
    ManagedChannelBuilder<?> cpBuilder = ManagedChannelBuilder.forTarget(targetEndpoint);

    ManagedChannel eefChannel = null;
    SpannerOptions.Builder spannerOptionsBuilder = SpannerOptions.newBuilder().setProjectId(projectId);

    if (fallbackEnabled) {
      GcpFallbackChannelOptions eefOptions =
          GcpFallbackChannelOptions.newBuilder()
              .setPrimaryChannelName("directpath")
              .setFallbackChannelName("cloudpath")
              .build();
      eefChannel = new GcpFallbackChannel(eefOptions, dpBuilder, cpBuilder);
      logger.log(Level.INFO, "GcpFallbackChannel enabled.");
      TransportChannelProvider channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(eefChannel));
      spannerOptionsBuilder.setChannelProvider(channelProvider);
    } else {
      logger.log(Level.INFO, "GcpFallbackChannel disabled. Using default channel provider.");
    }

    SpannerOptions spannerOptions = spannerOptionsBuilder.build();
    logger.log(Level.INFO, "Using credentials: " + spannerOptions.getCredentials());
    Spanner spanner = spannerOptions.getService();

    try {
      DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
      DatabaseClient dbClient =
          spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));

      //      createTableIfNotExists(dbAdminClient, instanceId, databaseId, 1);
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

