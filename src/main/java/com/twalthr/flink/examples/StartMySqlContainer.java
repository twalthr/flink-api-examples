package com.twalthr.flink.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/** Starts a MySQL docker container and initializes it tables and data. */
@SuppressWarnings({"resource", "InfiniteLoopStatement", "BusyWait"})
public class StartMySqlContainer {

  private static final Logger LOG = LoggerFactory.getLogger(StartMySqlContainer.class);

  private static final DockerImageName MYSQL_57_IMAGE = DockerImageName.parse("mysql:5.7.34");
  private static final String MYSQL_INIT_SCRIPT = "mysql-init.sql";

  private static final String MYSQL_PORT_FILE = "mysql-port.out";

  public static final Map<String, String> DEFAULT_CONTAINER_ENV_MAP =
      new HashMap<String, String>() {
        {
          put("MYSQL_ROOT_HOST", "%");
        }
      };

  public static void main(String[] args) throws InterruptedException, IOException {
    final MySQLContainer<?> container =
        new MySQLContainer<>(MYSQL_57_IMAGE)
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("mysql-config.cnf"), "/etc/mysql/conf.d/")
            .withUsername("root")
            .withPassword("")
            .withEnv(DEFAULT_CONTAINER_ENV_MAP)
            .withInitScript(MYSQL_INIT_SCRIPT)
            .withLogConsumer(new Slf4jLogConsumer(LOG));
    container.start();

    // export port
    final String port = container.getFirstMappedPort().toString();
    System.out.println("MySQL started at port: " + port);
    Files.write(Paths.get(MYSQL_PORT_FILE), port.getBytes(StandardCharsets.UTF_8));

    // busy waiting
    while (true) {
      Thread.sleep(200);
    }
  }

  public static String getPort() {
    try {
      return new String(Files.readAllBytes(Paths.get(MYSQL_PORT_FILE)), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
