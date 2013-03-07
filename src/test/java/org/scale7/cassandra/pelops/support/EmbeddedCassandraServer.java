package org.scale7.cassandra.pelops.support;

import java.io.File;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.CassandraDaemon;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

/**
 * This class starts and stops embedded Cassandra server.
 * 
 * For instance of server is created temporary directory (in target/tmp) for data files and configuration.
 * 
 * @author Alois Belaska <alois.belaska@gmail.com>
 */
public class EmbeddedCassandraServer {

	private final Logger logger = SystemProxy.getLoggerFromFactory(EmbeddedCassandraServer.class);

	private String baseDirectory = "target/tmp";

	private CassandraDaemon cassandraDaemon;

	private Thread cassandraThread;

	/**
	 * @return temporary base directory of running cassandra instance
	 */
	public String getBaseDirectory() {
		return baseDirectory;
	}

	/**
	 * starts embedded Cassandra server.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	public void start() throws Exception {
		try {
			cleanupDirectoriesFailover();

			FileUtils.createDirectory(baseDirectory);

			System.setProperty("log4j.configuration", "file:target/test-classes/log4j.properties");
			System.setProperty("cassandra.config", "file:target/test-classes/cassandra.yaml");

			cassandraDaemon = new CassandraDaemon();
			cassandraDaemon.init(null);
			cassandraThread = new Thread(new Runnable() {
				public void run() {
					try {
						cassandraDaemon.start();
					} catch (Exception e) {
						logger.error("Embedded casandra server run failed", e);
					}
				}
			});
			cassandraThread.setDaemon(true);
			cassandraThread.start();
		} catch (Exception e) {
			logger.error("Embedded casandra server start failed", e);

			// cleanup
			stop();
		}
	}

	/**
	 * Stops embedded Cassandra server.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	public void stop() throws Exception {
		if (cassandraThread != null) {
			cassandraDaemon.stop();
			cassandraDaemon.destroy();
			cassandraThread.interrupt();
			cassandraThread = null;
		}

		cleanupDirectoriesFailover();
	}

	/**
	 * Cleans up cassandra's temporary base directory.
	 * 
	 * In case o failure waits for 250 msecs and then tries it again, 3 times totally.
	 */
	public void cleanupDirectoriesFailover() {
		int tries = 3;
		while (tries-- > 0) {
			try {
				cleanupDirectories();
				break;
			} catch (Exception e) {
				// ignore exception
				try {
					Thread.sleep(250);
				} catch (InterruptedException e1) {
					// ignore exception
				}
			}
		}
	}

	/**
	 * Cleans up cassandra's temporary base directory.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	public void cleanupDirectories() throws Exception {
		File dirFile = new File(baseDirectory);
		if (dirFile.exists()) {
			FileUtils.deleteRecursive(dirFile);
		}
	}
}
