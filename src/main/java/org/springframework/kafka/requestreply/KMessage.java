package org.springframework.kafka.requestreply;

public class KMessage<V> {
	private byte[] correlationId;
	private V value;
	public byte[] getCorrelationId() {
		return correlationId;
	}
	public void setCorrelationId(byte[] correlationId) {
		this.correlationId = correlationId;
	}
	public V getValue() {
		return value;
	}
	public void setValue(V value) {
		this.value = value;
	}
}
