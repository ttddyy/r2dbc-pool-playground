package io.r2dbc.pool;

import java.time.Duration;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.pool.Pool;
import reactor.pool.PoolBuilder;
import reactor.pool.PooledRef;

/**
 *
 * @author Tadaya Tsuyukubo
 */
public class PooledConnectionFactory implements ConnectionFactory {

	private ConnectionFactory delegate;

	private Pool<Connection> pool;

	private PoolConfig config;

	// TODO: validate connection

	public PooledConnectionFactory(PoolConfig config, ConnectionFactory delegate) {
		this.delegate = delegate;
		this.config = config;

		// TODO: configure more (releaseHandler, eviction, etc)
		PoolBuilder<Connection> builder = PoolBuilder
				.from((Publisher<Connection>) this.delegate.create())
				.initialSize(config.getInitialSize())
				.sizeMax(config.getMaxSize());
		this.pool = builder.build();
	}

	@Override
	public Publisher<? extends Connection> create() {
		Mono<PooledConnection> mono = this.pool.acquire().map(PooledConnection::new);

		Duration acquireTimeout = this.config.getAcquireTimeout();
		if (acquireTimeout == null) {
			return mono;
		}
		return mono.timeout(acquireTimeout);
	}

	@Override
	public ConnectionFactoryMetadata getMetadata() {
		return this.delegate.getMetadata();
	}


	static class PooledConnection implements Connection {
		private Connection delegate;

		private PooledRef<Connection> pooledRef;

		public PooledConnection(PooledRef<Connection> pooledRef) {
			this.delegate = pooledRef.poolable();
			this.pooledRef = pooledRef;
		}

		@Override
		public Publisher<Void> close() {
			// just release to the pool and do NOT close the actual connection
			return this.pooledRef.release();
		}

		@Override
		public Publisher<Void> beginTransaction() {
			return this.delegate.beginTransaction();
		}

		@Override
		public Publisher<Void> commitTransaction() {
			return this.delegate.commitTransaction();
		}

		@Override
		public Batch createBatch() {
			return this.delegate.createBatch();
		}

		@Override
		public Publisher<Void> createSavepoint(String name) {
			return this.delegate.createSavepoint(name);
		}

		@Override
		public Statement createStatement(String sql) {
			return this.delegate.createStatement(sql);
		}

		@Override
		public Publisher<Void> releaseSavepoint(String name) {
			return this.delegate.releaseSavepoint(name);
		}

		@Override
		public Publisher<Void> rollbackTransaction() {
			return this.delegate.rollbackTransaction();
		}

		@Override
		public Publisher<Void> rollbackTransactionToSavepoint(String name) {
			return this.delegate.rollbackTransactionToSavepoint(name);
		}

		@Override
		public Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
			return this.delegate.setTransactionIsolationLevel(isolationLevel);
		}

	}

}
