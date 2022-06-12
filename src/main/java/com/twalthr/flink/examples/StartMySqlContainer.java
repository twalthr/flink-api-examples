package com.twalthr.flink.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"resource", "InfiniteLoopStatement", "BusyWait"})
public class StartMySqlContainer {

  private static final Logger LOG = LoggerFactory.getLogger(StartMySqlContainer.class);

  private static final DockerImageName MYSQL_57_IMAGE = DockerImageName.parse("mysql:5.7.34");
  private static final String MYSQL_INIT_SCRIPT = "mysql-init.sql";

  public static final Map<String, String> DEFAULT_CONTAINER_ENV_MAP =
      new HashMap<String, String>() {
        {
          put("MYSQL_ROOT_HOST", "%");
        }
      };

  static final MySQLContainer<?> MY_SQL_CONTAINER;

  static {
    MY_SQL_CONTAINER =
        new MySQLContainer<>(MYSQL_57_IMAGE)
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("mysql-config.cnf"), "/etc/mysql/conf.d/")
            .withUsername("root")
            .withPassword("")
            .withEnv(DEFAULT_CONTAINER_ENV_MAP)
            .withInitScript(MYSQL_INIT_SCRIPT)
            .withLogConsumer(new Slf4jLogConsumer(LOG));
    MY_SQL_CONTAINER.start();
  }

  public static void main(String[] args) throws InterruptedException {
    System.out.println("MySQL started:" + MY_SQL_CONTAINER.getJdbcUrl());
    while (true) {
      Thread.sleep(200);
    }
  }
}
