package io.r2dbc.pool;

import java.time.Duration;

import lombok.Getter;
import lombok.Setter;

/**
 *
 * @author Tadaya Tsuyukubo
 */
@Getter
@Setter
public class PoolConfig {

	private int initialSize = 0;

	private int maxSize = 10;

	private Duration acquireTimeout;
}
