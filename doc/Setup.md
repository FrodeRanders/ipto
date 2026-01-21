## Setup

### Database

We currently support either ```PostgreSQL``` or ```DB2``` (the LUW variant). 

The details in how we interface with these database management systems differs a bit, but for development
and test we use Docker. We have provided Python scripts that automates downloading the Docker images,
starting and setting up a clean database with the relevant schema.

```terminaloutput
➜  python db/postgresql/setup-for-test.py
Pulling the latest 'postgres' image from Docker Hub...
Using default tag: latest
latest: Pulling from library/postgres
b89cf3ec7a3e: Pull complete 
3d40ad1ee517: Pull complete 
5e86690e08b4: Pull complete 
36aea44977d6: Pull complete 
af5b2941599b: Pull complete 
e808f1e529cf: Pull complete 
07c5a27f4144: Pull complete 
d84932fbe51d: Pull complete 
87e5f1d7b994: Pull complete 
477d35ab7c57: Pull complete 
c598951473cf: Pull complete 
55bb4e0a9b60: Pull complete 
4aee7b1337ee: Pull complete 
Digest: sha256:5ec39c188013123927f30a006987c6b0e20f3ef2b54b140dfa96dac6844d883f
Status: Downloaded newer image for postgres:latest
docker.io/library/postgres:latest

What's next:
    View a summary of image vulnerabilities and recommendations → docker scout quickview postgres
Image pulled successfully.

Removing existing container named 'repo-postgres' (if any)...
Removed old container 'repo-postgres'.

Starting new container 'repo-postgres' on port 5432 ...
da12447c2ef70594b16646988ae3efd6ed374c23e289be928c6a262426bd91ff
Container started: da12447c2ef7

Awaiting startup...
Running psql commands in container...
Commands executed. Exiting psql.

All SQL files have been executed.

Setup completed.
```

### The backing management system

The system is configured by means of a properties configuration file used to point out the
database management system and its authentication details, together with a GraphQL SDL that
tells the system what attributes and types are available (discussed in [Configuration](Configuration.md)).

The configuration for using PostgreSQL (password matches mentioned DBMS-specific Python script):
```xml
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
<properties>

    <entry key="url">
        jdbc:postgresql://localhost:5432/repo
    </entry>

    <entry key="user">
        repo
    </entry>

    <entry key="password">
        repo
    </entry>

    <entry key="repository.database.adapter">
        org.gautelis.repo.search.query.adapters.PostgresAdapter
    </entry>

    <!--
        Non-db related configuration
    -->
    <entry key="repository.events.threshold">
        60
    </entry>

    <entry key="repository.events.listeners">
        org.gautelis.repo.listeners.NopActionListener
    </entry>

    <entry key="repository.cache.look_behind">
        true
    </entry>

    <entry key="repository.cache.max_size">
        1000
    </entry>

    <entry key="repository.cache.idle_check_interval">
        60
    </entry>

    <!--
        Unused, since we have a 'url'.
    -->
    <entry key="driver" />
    <entry key="manager" />
    <entry key="server" />
    <entry key="port" />
    <entry key="database" />
</properties>
```
The configuration for using DB2 (password matches mentioned DBMS-specific Python script):

```xml
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
<properties>

    <entry key="url">
        jdbc:db2://localhost:50000/REPO:currentSchema=REPO;currentFunctionPath=REPO,SYSIBM,SYSFUN,SYSPROC,SYSIBMADM;
    </entry>

    <entry key="user">
        DB2INST1
    </entry>

    <entry key="password">
        H0nd@666
    </entry>

    <entry key="repository.database.adapter">
        org.gautelis.repo.search.query.adapters.DB2Adapter
    </entry>

    <!--
        Non-db related configuration
    -->
    <entry key="repository.events.threshold">
        60
    </entry>

    <entry key="repository.events.listeners">
        org.gautelis.repo.listeners.NopActionListener
    </entry>

    <entry key="repository.cache.look_behind">
        true
    </entry>

    <entry key="repository.cache.max_size">
        1000
    </entry>

    <entry key="repository.cache.idle_check_interval">
        60
    </entry>

    <!--
        Unused, since we have a 'url'.
    -->
    <entry key="driver" />
    <entry key="manager" />
    <entry key="server" />
    <entry key="port" />
    <entry key="database" />
</properties>
```
### Initial startup

The initial startup is relatively simple, but it currently (!) depends on the configuration files being packaged
with the JAR-file; these being ```configuration.xml``` and ```sql-statements.xml```.

As such, startup is done -- simply -- like this.
```java
Repository repo = RepositoryFactory.getRepository();
```

Loading configuration is done like this.
```java
GraphQL graphQL = null;

try {
    try (InputStreamReader reader = new InputStreamReader(
        Objects.requireNonNull(this.getClass().getResourceAsStream("schema2.graphqls")))
    ) {
        Optional<GraphQL> _graphQL = Configurator.load(repo, reader, System.out);
    
        if (_graphQL.isEmpty()) {
            throw new RuntimeException("Failed to load configuration");
        }

        graphQL = _graphQL.get();
        ...
    }
} catch (Exception e) {
    throw new RuntimeException("Failed to initialize", e);
}
```

### Wiring GraphQL operations (the only required custom code)

The SDL describes the domain model and the GraphQL schema, but **operations** (queries and mutations) must be
wired to runtime behavior. This is done once at bootstrap time using `OperationsWireParameters`, and it is
the only piece of Java code a user of the platform needs to supply.

Two concrete examples live in the codebase:
* `it/src/test/java/org/gautelis/ipto/it/IptoSetupExtension.java` wires the test SDL.
* `repo-cdi/src/main/java/org/gautelis/ipto/bootstrap/IptoBootstrap.java` wires the Quarkus app SDL.

Both follow the same pattern:
1. Define the GraphQL operation signature as it appears in the SDL.
2. Convert input arguments to the generated model types (for example `Query.UnitIdentification`).
3. Call `RuntimeService` (available via `OperationsWireParameters`) to load, search, or store units.
4. Register a `DataFetcher` for the operation.

Minimal skeleton:
```java
static void wireOperations(OperationsWireParameters params) {
    String type = "Query";
    String operationName = "yourQuery";
    String parameterName = "id";

    DataFetcher<?> fetcher = env -> {
        Query.UnitIdentification id =
                MAPPER.convertValue(env.getArgument(parameterName), Query.UnitIdentification.class);
        return params.runtimeService().loadUnit(id.tenantId(), id.unitId());
    };

    params.runtimeWiring().type(type, t -> t.dataFetcher(operationName, fetcher));
}
```

If your SDL defines different operations, you must provide a matching wiring method that implements those
operations and routes them to the runtime service.

### Using built binaries (Quarkus app)

If you want to use the prebuilt Quarkus distribution as-is, you need to provide three things:
1. A database with the IPTO schema (see the database setup scripts earlier in this document).
2. A GraphQL SDL on the classpath (configured by `ipto.graphql.sdl-resource` in `application.yaml`).
3. A wiring method that matches the SDL's operations.

The default Quarkus app is configured in `quarkus-app/src/main/resources/application.yaml` and expects the SDL at
`/ipto.graphqls`. If you replace that SDL with your own, you must also update the wiring in
`repo-cdi/src/main/java/org/gautelis/ipto/bootstrap/IptoBootstrap.java` (or provide an equivalent bootstrap in
your own app) so that every `Query`/`Mutation` in the SDL is connected to a `DataFetcher`.

Example override:
```yaml
ipto:
  graphql:
    sdl-resource: /my-domain.graphqls
```

For custom systems, the recommended flow is:
1. Start from the test SDL and wiring (`it/src/test/resources/org/gautelis/ipto/it/schema2.graphqls` and
   `it/src/test/java/org/gautelis/ipto/it/IptoSetupExtension.java`).
2. Replace the SDL with your domain schema.
3. Implement the wiring method so each operation uses `RuntimeService` to load/search/store the right data.
4. Package the SDL as a resource in your app and set `ipto.graphql.sdl-resource` accordingly.
