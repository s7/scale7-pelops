package org.scale7.cassandra.pelops.support;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.RawColumnDefinition;
import org.apache.cassandra.config.RawColumnFamily;
import org.apache.cassandra.config.RawKeyspace;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.apache.cassandra.utils.SkipNullRepresenter;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;
import org.yaml.snakeyaml.Dumper;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Loader;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;

/**
 * This class starts and stops embedded Cassandra server.
 * 
 * For instance of server is created temporary directory (in
 * /tmp/pelops.cassandra.{TIMESTAMP}) for data files and configuration.
 * 
 * RPC and storage port can be set, including listen address.
 * 
 * From src/test/resource is copied log4.properties and cassandra.yaml with
 * modified parameters like commitlog_directory, data_file_directories,
 * listen_address, rpc_port, storage_port.
 * 
 * @author Alois Belaska <alois.belaska@gmail.com>
 */
@SuppressWarnings("deprecation")
public class EmbeddedCassandraServer {

	private final Logger logger = SystemProxy
			.getLoggerFromFactory(EmbeddedCassandraServer.class);

	private String listenAddress;

	private int rpcPort;

	private int storagePort;

	private String baseDirectory;

	private CassandraDaemon cassandraDaemon;

	private Thread cassandraThread;

	/**
	 * @return RPC port (default: 19160)
	 */
	public int getRpcPort() {
		return rpcPort;
	}

	/**
	 * @return storage port (default: 17000)
	 */
	public int getStoragePort() {
		return storagePort;
	}

	/**
	 * @return temporary base directory of running cassandra instance
	 */
	public String getBaseDirectory() {
		return baseDirectory;
	}

	public EmbeddedCassandraServer() {
		this("localhost", 19160, 17000);
	}

	public EmbeddedCassandraServer(int rpcPort) {
		this("localhost", rpcPort, 17000);
	}

	public EmbeddedCassandraServer(String listenAddress, int rpcPort) {
		this(listenAddress, rpcPort, 17000);
	}

	public EmbeddedCassandraServer(String listenAddress, int rpcPort,
			String baseDirectory) {
		this(listenAddress, rpcPort, 17000, baseDirectory);
	}

	public EmbeddedCassandraServer(String listenAddress, int rpcPort,
			int storagePort) {
		this(listenAddress, rpcPort, storagePort, "/tmp/pelops.cassandra."
				+ System.currentTimeMillis());
	}

	public EmbeddedCassandraServer(String listenAddress, int rpcPort,
			int storagePort, String baseDirectory) {
		this.listenAddress = listenAddress;
		this.rpcPort = rpcPort;
		this.storagePort = storagePort;
		this.baseDirectory = baseDirectory;
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

			copy("/log4j.properties", baseDirectory);
            System.setProperty("log4j.configuration",
                    new File(baseDirectory, "log4j.properties").toURI().toString());

			prepareConfiguration();

			System.setProperty("cassandra.config", "file:" + baseDirectory
					+ "/cassandra.yaml");

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
	 * In case o failure waits for 250 msecs and then tries it again, 3 times
	 * totally.
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

	/**
	 * Copies a resource from within the jar to a directory.
	 * 
	 * @param resource
	 *            recource to copy
	 * @param directory
	 *            destination directory
	 * @throws IOException
	 *             if an error occurs
	 */
	private static void copy(String resource, String directory)
			throws Exception {
		FileUtils.createDirectory(directory);
		InputStream is = EmbeddedCassandraServer.class
				.getResourceAsStream(resource);
		String fileName = resource.substring(resource.lastIndexOf("/") + 1);
		File file = new File(directory + System.getProperty("file.separator")
				+ fileName);
		OutputStream out = new FileOutputStream(file);
		byte buf[] = new byte[1024];
		int len;
		while ((len = is.read(buf)) > 0) {
			out.write(buf, 0, len);
		}
		out.close();
		is.close();
	}

	/**
	 * Copies modified cassandra configuration file to the base directory.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	private void prepareConfiguration() throws Exception {
		Config config = loadConfig("/cassandra.yaml");

		config.commitlog_directory = baseDirectory + "/commitlog";
		config.saved_caches_directory  = baseDirectory + "/saved_caches";
		config.listen_address = listenAddress;
		config.rpc_port = rpcPort;
		config.storage_port = storagePort;
		config.data_file_directories = new String[] { baseDirectory + "/data" };

		dumpConfig(config, baseDirectory + "/cassandra.yaml");
	}

	/**
	 * Load configuration from embedded yaml cassandra configuration file.
	 * 
	 * @param resource
	 *            Resource name.
	 * @return parsed cassandra configuration file
	 * @throws Exception
	 *             if an error occurs
	 */
	private Config loadConfig(String resource) throws Exception {
		InputStream is = EmbeddedCassandraServer.class
				.getResourceAsStream(resource);
		org.yaml.snakeyaml.constructor.Constructor constructor = new org.yaml.snakeyaml.constructor.Constructor(
				Config.class);
		TypeDescription desc = new TypeDescription(Config.class);
		desc.putListPropertyType("keyspaces", RawKeyspace.class);
		TypeDescription ksDesc = new TypeDescription(RawKeyspace.class);
		ksDesc.putListPropertyType("column_families", RawColumnFamily.class);
		TypeDescription cfDesc = new TypeDescription(RawColumnFamily.class);
		cfDesc.putListPropertyType("column_metadata", RawColumnDefinition.class);
		constructor.addTypeDescription(desc);
		constructor.addTypeDescription(ksDesc);
		constructor.addTypeDescription(cfDesc);
		Yaml yaml = new Yaml(new Loader(constructor));
		return (Config) yaml.load(is);
	}

	/**
	 * Dumps yaml configuration to file.
	 * 
	 * @param conf
	 *            The configuration to save.
	 * @param outfile
	 *            Output filename.
	 * @throws Exception
	 *             if an error occurs
	 */
	private void dumpConfig(Config conf, String outfile) throws Exception {
		DumperOptions options = new DumperOptions();
		/* Use a block YAML arrangement */
		options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
		SkipNullRepresenter representer = new SkipNullRepresenter();
		/* Use Tag.MAP to avoid the class name being included as global tag */
		representer.addClassTag(Config.class, Tag.MAP);
		representer.addClassTag(RawColumnFamily.class, Tag.MAP);
		Dumper dumper = new Dumper(representer, options);
		Yaml yaml = new Yaml(dumper);
		String output = yaml.dump(conf);

		/* Write to output file */
		BufferedWriter out = new BufferedWriter(new FileWriter(outfile));
		out.write("# Cassandra YAML generated for Pelops integration tests\n");
		out.write("# Configuration wiki: http://wiki.apache.org/cassandra/StorageConfiguration\n");
		out.write(output);
		out.close();
	}
}
