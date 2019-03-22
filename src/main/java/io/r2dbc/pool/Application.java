package io.r2dbc.pool;

import javax.sql.DataSource;

import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Flux;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.r2dbc.function.DatabaseClient;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Autowired
	DatabaseClient client;

	@GetMapping
	Flux<?> get() {
		return this.client.execute()
				.sql("SELECT value FROM test")
				.fetch()
				.all();
	}

	@Bean
	DataSource dataSource() {
		return new EmbeddedDatabaseBuilder()
				.setType(EmbeddedDatabaseType.H2)
				.build();
	}

	@Bean
	CommandLineRunner bootstrap(DataSource dataSource) {
		return args -> {
			JdbcOperations jdbcOperations = new JdbcTemplate(dataSource);
			jdbcOperations.execute("DROP TABLE IF EXISTS test");
			jdbcOperations.execute("CREATE TABLE test ( value INTEGER )");
			jdbcOperations.execute("INSERT INTO test VALUES (100)");
			jdbcOperations.execute("INSERT INTO test VALUES (200)");

			// create sleep function for slow query
			jdbcOperations.execute("CREATE ALIAS SLEEP FOR \"java.lang.Thread.sleep(long)\"");
		};
	}

	@Bean
	DatabaseClient databaseClient(ConnectionFactory connectionFactory) {
		return DatabaseClient.create(connectionFactory);
	}

	@Bean
	PoolConfig config() {
		return new PoolConfig();  // config object
	}

	@Bean
	ConnectionFactory connectionFactory(PoolConfig config) {
		H2ConnectionConfiguration h2Configuration = H2ConnectionConfiguration.builder()
				.username("sa")
				.password("")
				.inMemory("testdb")
				.build();


		ConnectionFactory connectionFactory = new H2ConnectionFactory(h2Configuration);

		PooledConnectionFactory pooledConnectionFactory = new PooledConnectionFactory(config, connectionFactory);

		return pooledConnectionFactory;
	}

}
