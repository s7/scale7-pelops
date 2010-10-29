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
        		<id>maven.spidertracks.com</id>
        		<name>Scale7 Maven Repo</name>
        		<url>http://github.com/tnine/m2repo/raw/master</url>
		        <snapshots>
		            <enabled>true</enabled>
		        </snapshots>
		    </repository>
		</repositories>

2. Choose the appropriate Pelops version. We're using the latest trunk, which is the 0.7.0 rc1 source.  The nightly hudson build is included.

		<dependency>
			<groupId>org.scale7</groupId>
			<artifactId>scale7-pelops</artifactId>
			<version>0.907-0.7.0-st-SNAPSHOT</version>
		</dependency>


3. Start using Pelops.


More Resources:
[Mailing List](http://groups.google.com/group/scale7)