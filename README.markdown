Pelops has been created to make working with Cassandra a beautiful thing (hence the nickname "Cassandra's beautiful son").
Using Pelops developers can quickly access the full power of Cassandra while writing clean, self-documenting code that
makes the underlying semantics clear to reviewers. Without compromising power, Pelops has been designed to improve
productivity and code quality while greatly reducing the learning curve for new users. In fact, these objectives go
hand in hand.

To use pelops in a maven project:
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
2. Add the following dep to your project:
    <dependency>
        <groupId>org.scale7</groupId>
        <artifactId>scale7-pelops</artifactId>
        <version>0.903-0.7.0-SNAPSHOT</version>
    </dependency>
3. Start using pelops.
