package cn.edu.tsinghua.iotdb.benchmark.function;

import java.util.Random;

public class Function {
	private Random random;

	public Function(int seed) {
		this.random = new Random(seed);
	}

	public Number get(long timestamp) {
		return 1000 * Math.sin(random.nextDouble() * timestamp) + 1000;
	}
}
