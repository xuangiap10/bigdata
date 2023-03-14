package part1.averageInMapper;

public class Pair<K,V> {

	private K key;
	private V value;
	
	Pair(K _key, V _value) { 
		key = _key;
		value = _value;
	}
	
	public K getKey() {return key;}
	public V getValue() {return value;}
	public void setKey(K _key) {key = _key;}
	public void setValue(V _value) {value = _value;}
}

