package diskBackedMap;

import java.io.Serializable;

public class MapEntry<K,V> implements Serializable{
	
	private  K key;
	private V value;
	
		public MapEntry(K key,V value) {
		// TODO Auto-generated constructor stub
		this.key= key;
		this.value= value;
		}
	
		public K getKey() {
	        return key;
	    }

	    public V getValue() {
	        return value;
	    }

	    public void setValue(V value) {
	        this.value = value;
	    }
	

}
