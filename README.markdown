Introduction
=========
Pelops has been created to make working with Cassandra a beautiful thing (hence the nickname "Cassandra's beautiful son").
Using Pelops developers can quickly access the full power of Cassandra while writing clean, self-documenting code that
makes the underlying semantics clear to reviewers. Without compromising power, Pelops has been designed to improve
productivity and code quality while greatly reducing the learning curve for new users. In fact, these objectives go
hand in hand.

License
======
Pelops is provided under the [MIT licence](http://www.opensource.org/licenses/mit-license.php). 

Releases
=====================
The latest stable release is 1.3-1.1.x, the latest SNAPSHOT is 1.4-1.0.x-SNAPSHOT and can be downloaded (along with all versions) from: https://github.com/s7/mvnrepo/tree/master/org/scale7/scale7-pelops

Using Pelops in a Maven project
========================
To use Pelops with Cassandra 1.1.x or 1.2.x use the following:

1. Add the following repository to your project repositories (note, the maven.scale7.org repo isn't nexus friendly):

        <repositories>
            <repository>
                <id>central</id>
                <name>Maven Central Repo</name>
                <url>http://repo1.maven.org/maven2</url>
            </repository>
            <repository>
                <id>riptano</id>
                <name>riptano</name>
                <url>http://mvn.riptano.com/content/repositories/public</url>
            </repository>
            <repository>
                <id>maven.scale7.org</id>
                <name>Scale7 Maven Repo</name>
                <url>https://github.com/s7/mvnrepo/raw/master</url>
                <snapshots>
                    <enabled>true</enabled>
                </snapshots>
            </repository>
        </repositories>

2a. Add the Pelops dependency to your project (depends on Cassandra 1.1.x):

		<dependency>
			<groupId>org.scale7</groupId>
			<artifactId>scale7-pelops</artifactId>
			<version>1.3-1.1.x</version>
		</dependency>

3. Create your keyspace and column families using [cassandra-cli](http://wiki.apache.org/cassandra/CassandraCli).

        create keyspace mykeyspace with replication_factor = 1 and placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy';
        use mykeyspace;
        create column family users with column_type = 'Standard' and comparator = 'UTF8Type';


4. Play around with this simple example.

        /*
          NOTE: This example uses the static Pelops methods because they are more concise.  If you'd prefer not
                to work with static methods you can construct an instance of IThriftPool and use its instance
                methods instead.
                e.g. IThriftPool pool = new CommonsBackedPool(cluster, keyspace)
        */
        String pool = "pool";
        String keyspace = "mykeyspace";
        String colFamily = "users";

        // init the connection pool
        Cluster cluster = new Cluster("localhost", 9160);
        Pelops.addPool(pool, cluster, keyspace);

        String rowKey = "abc123";
        
        // write out some data
        Mutator mutator = Pelops.createMutator(pool);
        mutator.writeColumns(
                colFamily, rowKey,
                mutator.newColumnList(
                        mutator.newColumn("name", "Dan"),
                        mutator.newColumn("age", Bytes.fromInt(33))
                )
        );
        mutator.execute(ConsistencyLevel.ONE);
        
        // read back the data we just wrote
        Selector selector = Pelops.createSelector(pool);
        List<Column> columns = selector.getColumnsFromRow(colFamily, rowKey, false, ConsistencyLevel.ONE);
        
        System.out.println("Name: " + Selector.getColumnStringValue(columns, "name"));
        System.out.println("Age: " + Selector.getColumnValue(columns, "age").toInt());
        
        // shut down the pool
        Pelops.shutdown();
        

A note on dependencies
----------------------------------
Pelops requires the following dependencies at runtime.  These dependencies are included as [transitive dependencies](http://maven.apache.org/guides/introduction/introduction-to-dependency-mechanism.html#Transitive_Dependencies) without any extra work from you.

* org.slf4j:slf4j-api - version: 1.5.11
* com.eaio.uuid:uuid - version: 3.2
* org.scale7:scale7-core -version: 1.3
* org.apache.cassandra:cassandra-thrift - version: 1.1.0
*   -> org.apache.cassandra:cassandra-thrift - version: 1.1.0
*   -> org.apache.thrift.libthrift - version: 0.7.0
* commons-pool:commons-pool - version: 1.6

Pelops also includes the following **[optional dependencies](http://maven.apache.org/guides/introduction/introduction-to-optional-and-excludes-dependencies.html)** that are **not required or included unless you explicitly want/need them**.  The classes and methods that require these optional dependencies are clearly Javadoc'ed so you shouldn't have any trouble figuring out when to include them.

* org.springframework:spring-context - version: 3.0.5.RELEASE (only required by classes in the org.scale7.cassandra.pelops.spring package)
* joda-time:joda-time -version: 1.6.2 (only required a few helper methods on the UuidHelper class)

More Resources
============
* [Mailing List](http://groups.google.com/group/scale7)
* [Introduction on Bits and Bytes](http://ria101.wordpress.com/2010/06/11/pelops-the-beautiful-cassandra-database-client-for-java/)

Projects that use Pelops
============
* [Fight My Monster](http://www.fightmymonster.com/)
* [Digital Pigeon](https://digitalpigeon.com)
