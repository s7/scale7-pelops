Pelops has been created to make working with Cassandra a beautiful thing (hence the nickname "Cassandra's beautiful son").
Using Pelops developers can quickly access the full power of Cassandra while writing clean, self-documenting code that
makes the underlying semantics clear to reviewers. Without compromising power, Pelops has been designed to improve
productivity and code quality while greatly reducing the learning curve for new users. In fact, these objectives go
hand in hand.

To use Pelops with Cassandra 0.7.x use the following:

1. Add the following repository to your project repositories:

		<repositories>
			<repository>
				<id>central</id>
				<name>Maven Central Repo</name>
				<url>http://repo1.maven.org/maven2</url>
			</repository>
			<repository>
				<id>maven.scale7.org</id>
				<name>Scale7 Maven Repo</name>
				<url>http://github.com/s7/mvnrepo/raw/master</url>
				<snapshots>
					<enabled>true</enabled>
				</snapshots>
			</repository>
		</repositories>

2a. If you're using Cassandra 0.7.0-beta1 then add the following dep to your project:

		<dependency>
			<groupId>org.scale7</groupId>
			<artifactId>scale7-pelops</artifactId>
			<version>0.903-0.7.0-SNAPSHOT</version>
		</dependency>

2b. If Cassandra 0.7.0-beta1 isn't bleeding edge enough for you then add the following dep to your project
and use the latest build available from
[Hudson](https://hudson.apache.org/hudson/job/Cassandra/lastSuccessfulBuild/artifact/cassandra/build/) (apache-cassandra-2010-09-22_13-09-02-bin.tar.gz):

		<dependency>
			<groupId>org.scale7</groupId>
			<artifactId>scale7-pelops</artifactId>
			<version>0.906-0.7.0-SNAPSHOT</version>
		</dependency>

Note: this version is the only one getting Pelops fixes and updates.

3. Start using Pelops.

Cassandra 0.6.x users should use:

		<dependency>
			<groupId>org.wyki</groupId>
			<artifactId>pelops</artifactId>
			<version>0.805-0.6.4</version>
		</dependency>

More Resources:
[Mailing List](http://groups.google.com/group/scale7)