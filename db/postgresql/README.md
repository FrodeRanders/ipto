# Data model

## Model
```mermaid
erDiagram
    repo_tenant {
        INT tenantid PK
        TEXT name UK
        TEXT description
        TIMESTAMP created
    }

    repo_unit_kernel {
        INT tenantid PK, FK
        BIGINT unitid PK
        UUID corrid UK
        INT status
        INT lastver
        TIMESTAMP created
    }

    repo_unit_version {
        INT tenantid PK, FK
        BIGINT unitid PK, FK
        INTEGER unitver PK
        VARCHAR(255) unitname
        TIMESTAMP modified
    }

    repo_namespace {
        TEXT alias PK
        TEXT namespace PK
    }

    repo_attribute {
        INT attrid PK
        TEXT qualname UK
        TEXT attrname UK
        TEXT alias
        INT attrtype
        BOOLEAN scalar
        TIMESTAMP created
    }

    repo_attribute_description {
        INT attrid PK, FK
        CHAR(2) lang PK
        TEXT alias
        TEXT description
    }

    repo_attribute_value {
        INT tenantid PK, FK
        BIGINT unitid PK, FK
        INT attrid PK, FK
        BIGINT valueid PK, UK
        INTEGER unitverfrom PK, FK
        INTEGER unitverto FK
    }

    repo_unit_template {
        INT templateid PK
        TEXT name UK
    }

    repo_unit_template_elements {
        INT templateid PK, FK
        INT attrid PK
        INT idx
        TEXT alias
    }

    repo_record_template {
        INT recordid PK, FK
        TEXT name UK
    }

    repo_record_template_elements {
        INT recordid PK, FK
        INT attrid PK, FK
        INT idx
        TEXT alias
    }

    repo_string_vector {
        BIGINT valueid PK, FK
        INT idx PK
        TEXT value
    }

    repo_time_vector {
        BIGINT valueid PK, FK
        INT idx PK
        TIMESTAMP value
    }

    repo_integer_vector {
        BIGINT valueid PK, FK
        INT idx PK
        INT value
    }

    repo_long_vector {
        BIGINT valueid PK, FK
        INT idx PK
        BIGINT value
    }

    repo_double_vector {
        BIGINT valueid PK, FK
        INT idx PK
        DOUBLE value
    }

    repo_boolean_vector {
        BIGINT valueid PK, FK
        INT idx PK
        BOOLEAN value
    }

    repo_data_vector {
        BIGINT valueid PK, FK
        INT idx PK
        BYTEA value
    }

    repo_record_vector {
        BIGINT valueid PK, FK
        INT idx PK
        INT ref_attrid
        BIGINT ref_valueid FK
    }

    repo_log {
        INT tenantid
        BIGINT unitid
        INTEGER unitver
        INT event
        TEXT logentry
        TIMESTAMP logtime
    }

    repo_lock {
        INT tenantid PK, FK
        BIGINT unitid PK, FK
        BIGINT lockid PK
        TEXT purpose
        INT locktype
        TIMESTAMP expire
        TIMESTAMP locktime
    }

    repo_internal_relation {
        INT tenantid PK, FK
        BIGINT unitid PK, FK
        INT reltype PK
        INT reltenantid PK, FK
        BIGINT relunitid PK, FK
        TIMESTAMP created
    }

    repo_external_assoc {
        INT tenantid PK, FK
        BIGINT unitid PK, FK
        INT assoctype PK
        TEXT assocstring PK
        TIMESTAMP created
    }

    repo_tenant ||--o{ repo_unit_kernel : "tenantid"
    repo_unit_kernel ||--o{ repo_unit_version : "tenantid, unitid"
    repo_attribute ||--o{ repo_attribute_description : "attrid"
    repo_attribute ||--o{ repo_attribute_value : "attrid"
    repo_unit_version ||--o{ repo_attribute_value : "tenantid, unitid, unitverfrom"
    repo_unit_version ||--o{ repo_attribute_value : "tenantid, unitid, unitverto"
    repo_unit_template ||--o{ repo_unit_template_elements : "templateid"
    repo_attribute ||--o{ repo_record_template : "recordid"
    repo_attribute ||--o{ repo_record_template_elements : "attrid"
    repo_record_template ||--o{ repo_record_template_elements : "recordid"
    repo_attribute_value ||--o{ repo_string_vector : "valueid"
    repo_attribute_value ||--o{ repo_time_vector : "valueid"
    repo_attribute_value ||--o{ repo_integer_vector : "valueid"
    repo_attribute_value ||--o{ repo_long_vector : "valueid"
    repo_attribute_value ||--o{ repo_double_vector : "valueid"
    repo_attribute_value ||--o{ repo_boolean_vector : "valueid"
    repo_attribute_value ||--o{ repo_data_vector : "valueid"
    repo_attribute_value ||--o{ repo_record_vector : "valueid"
    repo_attribute_value ||--o{ repo_record_vector : "ref_valueid"
    repo_unit_kernel ||--o{ repo_lock : "tenantid, unitid"
    repo_unit_kernel ||--o{ repo_internal_relation : "tenantid, unitid"
    repo_unit_kernel ||--o{ repo_internal_relation : "reltenantid, relunitid"
    repo_unit_kernel ||--o{ repo_external_assoc : "tenantid, unitid"
```
## Mermaid ERD generator
Generate this Mermaid diagram from schema.sql:

```
python generate_schema_mermaid.py --schema schema.sql --markdown
```

Or update this README in-place:

```
python generate_schema_mermaid.py --schema schema.sql --update-readme README.md
```

The script includes its own embedded README at the top of `generate_schema_mermaid.py`.

# Instructions for retrieving, running and preparing PostgreSQL

The name of the database ('repo'), the user ('repo') and the password ('repo'), 
matches the [configuration](../../repo/src/main/resources/org/gautelis/ipto/repo/configuration_pg.xml) 
that is currently stored among the resources. This is not ideal for production use, 
but makes demonstrating the functionality a breeze.

## Retrieving PostgreSQL from Docker hub

```
~ > docker pull postgres
Using default tag: latest
latest: Pulling from library/postgres
2cc3ae149d28: Pull complete
d1a63825d58e: Pull complete
ed6f372fe58d: Pull complete
35f975e69306: Pull complete
40c4fe86e99d: Pull complete
4795e1a32ff6: Pull complete
bcb5a54ae87d: Pull complete
d3983228bec6: Pull complete
5378bf7229e9: Pull complete
bba3241011a6: Pull complete
5e1d0413d05a: Pull complete
6a489170d05e: Pull complete
440b39aff272: Pull complete
582c79113570: Pull complete
Digest: sha256:46aa2ee5d664b275f05d1a963b30fff60fb422b4b594d509765c42db46d48881
Status: Downloaded newer image for postgres:latest
docker.io/library/postgres:latest

What's next:
    View a summary of image vulnerabilities and recommendations → docker scout quickview postgres
```

## Start an instance of PostgreSQL

```
~ > docker run --name repo-postgres -e POSTGRES_PASSWORD=H0nd@666 -p 1402:5432 -d postgres
2dddb1bbf6f4684ba4a44fccb92d4b5f924902c4920e84b7ed7aa84caa165300

~ > docker ps -all
CONTAINER ID   IMAGE      COMMAND                  CREATED          STATUS          PORTS                    NAMES
2dddb1bbf6f4   postgres   "docker-entrypoint.s…"   25 seconds ago   Up 24 seconds   0.0.0.0:1402->5432/tcp   repo-postgres
```

## Preparing database

You are absolutely right -- 'repo' is not a good password for user 'repo', not even in an example.

```
~ > docker run -it --rm --link repo-postgres:postgres postgres psql -h postgres -U postgres
Password for user postgres: 

psql (16.3 (Debian 16.3-1.pgdg120+1))
Type "help" for help.

postgres=# CREATE USER repo WITH PASSWORD 'repo';
CREATE ROLE

postgres=# CREATE DATABASE repo;
CREATE DATABASE

postgres=# ALTER DATABASE repo OWNER TO repo;
ALTER DATABASE

postgres=# \q
```

## More at https://hub.docker.com/_/postgres/

