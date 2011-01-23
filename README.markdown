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

Using Pelops in a Maven project
========================
To use Pelops with Cassandra 0.7.x use the following:

1. Add the following repository to your project repositories:

		<repositories>
			<repository>
				<id>central</id>
				<name>Maven Central Repo</name>
				<url>http://repo1.maven.org/maven2</url>
			</repository>
            <repository>
                <id>riptano</id>
                <name>riptano</name>
                <url>http://mvn.riptano.com/content/repositories/public/</url>
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

2. Add the Pelops dependency to your project (depends on Cassandra 0.7.0)):

		<dependency>
			<groupId>org.scale7</groupId>
			<artifactId>scale7-pelops</artifactId>
			<version>1.0-RC1-0.7.0-SNAPSHOT</version>
		</dependency>

    Note: this version is the only one getting Pelops fixes and updates.

3. Start using Pelops.

A note on dependencies
----------------------------------
Pelops requires the following dependencies at runtime.  These dependencies are included as [transitive dependencies](http://maven.apache.org/guides/introduction/introduction-to-dependency-mechanism.html#Transitive_Dependencies) without any extra work from you.

* org.slf4j:slf4j-api - version: 1.5.11
* com.eaio.uuid:uuid - version: 3.2
* org.scale7:scale7-core -version: 1.3
* org.apache.cassandra:apache-cassandra - version: 0.7.0 (pulled from the Riptano/Datastax Maven repo)
* libthrift:libthrift - version: 0.5
* commons-pool:commons-pool -version:1.5.5

Pelops also includes the following **[optional dependencies](http://maven.apache.org/guides/introduction/introduction-to-optional-and-excludes-dependencies.html)** that are **not required or included unless you explicitly want/need them**.  The classes and methods that require these optional dependencies are clearly Javadoc'ed so you shouldn't have any trouble figuring out when to include them.

* org.springframework:spring-context - version: 3.0.5.RELEASE (only required by classes in the org.scale7.cassandra.pelops.spring package)
* joda-time:joda-time -version: 1.6.2 (only required a few helper methods on the UuidHelper class)

Cassandra 0.6.x users can use:

		<dependency>
			<groupId>org.wyki</groupId>
			<artifactId>pelops</artifactId>
			<version>0.805-0.6.4</version>
		</dependency>
**Please note that the 0.6.x version is not maintained and has known issues**.  These issues have been fixed in the 0.7.x version.  If you're interested in helping maintaining the 0.6.x branch let us know...

More Resources:
[Mailing List](http://groups.google.com/group/scale7)