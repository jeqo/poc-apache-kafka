# Kafka HTTP Producer

## How to run

Add `kafka.properties` file with connection properties to: `src/main/jib`.

Then, build local image:

```shell
mvn clean compile jib:dockerBuild
```

And start docker-compose:

```shell
docker-compose up -d
```

Go to <http://localhost:8080> and test the `POST /send` API.
