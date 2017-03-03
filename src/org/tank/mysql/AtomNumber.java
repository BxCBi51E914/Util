package org.tank.mysql;

/**
 * Created by Tank
 * on 2017/1/10.
 */
public class AtomNumber {
	private int number = 0;

	public AtomNumber() {
		this.number = 0;
	}

	public AtomNumber(int number) {
		this.number = number;
	}

	public void inc(int n) {
		synchronized (this) {
			number += n;
		}
	}

	public int getValue() {
		return number;
	}

	public void setValue(int number) {
		synchronized (this) {
			this.number = number;
		}
	}
}
