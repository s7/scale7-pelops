Pelops has been created to make working with Cassandra a beautiful thing (hence the nickname "Cassandra's beautiful son").
Using Pelops developers can quickly access the full power of Cassandra while writing clean, self-documenting code that
makes the underlying semantics clear to reviewers. Without compromising power, Pelops has been designed to improve
productivity and code quality while greatly reducing the learning curve for new users. In fact, these objectives go
hand in hand.

Pelops is provided under the [MIT licence](http://www.opensource.org/licenses/mit-license.php). 

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

2. Add the Pelops dependency to your project (currently depends on Cassandra 0.7.0-rc3)):

		<dependency>
			<groupId>org.scale7</groupId>
			<artifactId>scale7-pelops</artifactId>
			<version>0.912-0.7.0-SNAPSHOT</version>
		</dependency>

    Note: this version is the only one getting Pelops fixes and updates.

3. Start using Pelops.



Cassandra 0.6.x users can use:

		<dependency>
			<groupId>org.wyki</groupId>
			<artifactId>pelops</artifactId>
			<version>0.805-0.6.4</version>
		</dependency>
**Please note that the 0.6.x version is not maintained and has known issues**.  These issues have been fixed in the 0.7.x version.  If you're interested in helping maintaining the 0.6.x branch let us know...

More Resources:
[Mailing List](http://groups.google.com/group/scale7)