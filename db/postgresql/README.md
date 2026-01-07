# Data model

```mermaid
erDiagram
%% =========================
%% CORE / TENANCY & UNITS
%% =========================
    repo_tenant {
        INT tenantid PK
        TEXT name
        TEXT description
        TIMESTAMP created
    }

    repo_unit_kernel {
        INT tenantid FK
        BIGINT unitid PK
        UUID corrid
        INT status
        INT lastver
        TIMESTAMP created
    }

    repo_unit_version {
        INT tenantid FK
        BIGINT unitid FK
        INT unitver PK
        VARCHAR unitname
        TIMESTAMP modified
    }

    repo_lock {
        INT tenantid FK
        BIGINT unitid FK
        BIGINT lockid PK
        TEXT purpose
        INT locktype
        TIMESTAMP expire
        TIMESTAMP locktime
    }

    repo_internal_assoc {
        INT tenantid FK           
        BIGINT unitid FK          
        INT assoctype
        INT assoctenantid FK      
        BIGINT assocunitid FK     
    }

    repo_external_assoc {
        INT tenantid FK
        BIGINT unitid FK
        INT assoctype
        TEXT assocstring
    }

%% Relationships (core)
    repo_tenant ||--o{ repo_unit_kernel : "has"
    repo_unit_kernel ||--o{ repo_unit_version : "versions"
    repo_unit_kernel ||--o{ repo_lock : "locks"
    repo_unit_kernel ||--o{ repo_internal_assoc : "left relation"
    repo_unit_kernel ||--o{ repo_internal_assoc : "right relation"
    repo_unit_kernel ||--o{ repo_external_assoc : "external association"

%% =========================
%% ATTRIBUTE CATALOG
%% =========================
    repo_namespace {
        TEXT alias
        TEXT namespace
    }

    repo_attribute {
        INT attrid PK
        TEXT qualname
        TEXT attrname
        INT attrtype
        BOOLEAN scalar
        TIMESTAMP created
    }

    repo_attribute_description {
        INT attrid FK
        CHAR lang
        TEXT alias
        TEXT description
    }

    repo_record_template {
        INT record_attrid FK       
        INT idx
        INT child_attrid          
        TEXT alias
        BOOLEAN required
    }

    repo_unit_template {
        INT templateid PK
        TEXT name
    }

    repo_template_elements {
        INT templateid FK
        INT attrid                 
        TEXT alias
        INT idx
    }

%% Relationships (attribute catalog)
    repo_attribute ||--o{ repo_attribute_description : "descriptions"
    repo_attribute ||--o{ repo_record_template : "record defs"
    repo_unit_template ||--o{ repo_template_elements : "elements"

%% =========================
%% ATTRIBUTE VALUES & VECTORS
%% =========================
repo_attribute_value {
    INT tenantid FK
    BIGINT unitid FK
    INT attrid FK
    INT unitverfrom FK         
    INT unitverto   FK         
    BIGINT valueid 
}

repo_string_vector {
    BIGINT valueid FK
    INT idx
    TEXT value
}

repo_time_vector {
    BIGINT valueid FK
    INT idx
    TIMESTAMP value
}

repo_integer_vector {
    BIGINT valueid FK
    INT idx
    INT value
}

repo_long_vector {
    BIGINT valueid FK
    INT idx
    BIGINT value
}

repo_double_vector {
    BIGINT valueid FK
    INT idx
    DOUBLE value
}

repo_boolean_vector {
    BIGINT valueid FK
    INT idx
    BOOLEAN value
}

repo_data_vector {
    BIGINT valueid FK
    INT idx
    BYTEA value
}

repo_record_vector {
    BIGINT valueid FK          
    INT idx
    INT ref_attrid             
    BIGINT ref_valueid FK      
}

%% Relationships (values)
repo_attribute ||--o{ repo_attribute_value : "has values"
repo_unit_version ||--o{ repo_attribute_value : "from version"
repo_unit_version ||--o{ repo_attribute_value : "to version"

repo_attribute_value ||--o{ repo_string_vector  : "strings"
repo_attribute_value ||--o{ repo_time_vector    : "timestamps"
repo_attribute_value ||--o{ repo_integer_vector : "integers"
repo_attribute_value ||--o{ repo_long_vector    : "longs"
repo_attribute_value ||--o{ repo_double_vector  : "doubles"
repo_attribute_value ||--o{ repo_boolean_vector : "booleans"
repo_attribute_value ||--o{ repo_data_vector    : "binary"
repo_attribute_value ||--o{ repo_record_vector  : "records (child via ref_valueid)"
repo_attribute_value ||--o{ repo_record_vector  : "records (parent)"
```

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

