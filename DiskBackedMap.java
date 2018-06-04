package diskBackedMap;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DiskBackedMap<K,V> implements Map<K, V>{
	
	private int size;
	private int internalSize;
	private static int filenumber;
	private int thresholdValue=2;
	private final String filePath="/var/tmp/myObjects"+filenumber+".txt";
	private HashSet<MapEntry<K, V>>  internalSet;
	private boolean onDisk=false;

	
	public  DiskBackedMap() {
		// TODO Auto-generated constructor stub
	
	internalSet = new HashSet<MapEntry<K,V>>();
	filenumber ++;
	}
	
	
	
	@Override
	public int size() {
		// TODO Auto-generated method stub
		return size;
	}

	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		if(size>0) {
			return true;
		}
		return false;
	}

	@Override
	public boolean containsKey(Object key) {
		if (size>0) {
			// TODO Auto-generated method stub
			for (MapEntry<K, V> mapEntry : internalSet) {
				
				if(mapEntry.getKey().equals(key)) {
					return true;
				}
				
			} 
			if (onDisk) {
				HashSet<MapEntry<K, V>> tempData = readDiskData();
				for (MapEntry<K, V> mapEntry : tempData) {
					if(mapEntry.getKey().equals(key)) {
						return true;
					}
				} 
			}
			
		}
		return false;
	}

	@Override
	public boolean containsValue(Object value) {
		// TODO Auto-generated method stub
		
		if (size>0) {
			// TODO Auto-generated method stub
			for (MapEntry<K, V> mapEntry : internalSet) {
				
				if(mapEntry.getValue().equals(value)) {
					return true;
				}
				
			} 
			if (onDisk) {
				HashSet<MapEntry<K, V>> tempData = readDiskData();
				for (MapEntry<K, V> mapEntry : tempData) {
					if(mapEntry.getValue().equals(value)) {
						return true;
					}
				} 
			}
			
		}
		
		return false;
	}

	@Override
	synchronized public V get(Object key) {
		if (internalSet !=null && internalSet.size() > 0) {
			// TODO Auto-generated method stub
			for (MapEntry<K, V> mapEntry : internalSet) {
				if (key.equals(mapEntry.getKey()) && key.hashCode()== mapEntry.getKey().hashCode()) {
					return mapEntry.getValue();

				}

			}
			if (onDisk) {
				HashSet<MapEntry<K, V>> diskData = readDiskData();
				for (MapEntry<K, V> mapEntry : diskData) {
					if (key.equals(mapEntry.getKey()) && key.hashCode()== mapEntry.getKey().hashCode() ) {
						return mapEntry.getValue();

					}
				}
			} 
		}
		return null;
	}

	@Override
	synchronized public V put(K key, V value) {
		// TODO Auto-generated method stub
		MapEntry<K, V> newEntry= new MapEntry<K,V>(key, value);
		boolean duplicate = false;

		
		if(internalSet !=null && internalSize >0 ) {
			
				for (MapEntry<K, V> mapEntry : internalSet) {
					
					if(mapEntry.getKey().hashCode() == key.hashCode() && mapEntry.getKey().equals(key)) {
						
						internalSet.remove(mapEntry);
						internalSet.add(newEntry);
						duplicate = true;
						break;
					}
					
					
				}
				
				if(!duplicate && internalSize < thresholdValue) {
					
					internalSet.add(newEntry);
					internalSize++;
					size++;
					
				}else if(!duplicate) {
					
					if (!(internalSize == thresholdValue) || onDisk) {
						HashSet<MapEntry<K, V>> diskData = readDiskData();
						for (MapEntry<K, V> mapEntry : diskData) {

							if (mapEntry.getKey().hashCode() == key.hashCode() && mapEntry.getKey().equals(key)) {

								diskData.remove(mapEntry);
								diskData.add(newEntry);
								duplicate = true;
								writeEntryObject(diskData);
								break;
							}

						}
						if (!duplicate) {

							diskData.add(newEntry);
							writeEntryObject(diskData);
							size++;

						} 
					}else {
						
						HashSet<MapEntry<K, V>> newData = new HashSet<MapEntry<K,V>>();
						newData.add(newEntry);
						initializeDiskEntry(newData);
						size++;
						
					}
					
					
					
				}
				
			
			
		}else {
			
			internalSet.add(newEntry);
			internalSize++;
			size++;
		}
		
		
		
		return value;
	}

	@Override
	public V remove(Object key) {
		// TODO Auto-generated method stub
		V value=null;
	
		if(internalSet != null && internalSet.size() > 0) {
			
			for (MapEntry<K, V> mapEntry : internalSet) {
				
				if(mapEntry.getKey().equals(key) && mapEntry.getKey().hashCode() == key.hashCode()) {
					
					internalSet.remove(mapEntry);
					size--;
					internalSize--;
					rearangeObjects();
					break;
					
				}
				
				
			}
			
		}
		
		return value;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		// TODO Auto-generated method stub
		Set<? extends K> temp =m.keySet();
		for (K k : temp) {
			this.put(k, m.get(k));
			
		}
		
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		internalSet.clear();
		try {
			finalize();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		size=0;
		internalSize=0;
		
	}

	@Override
	public Set<K> keySet() {
		// TODO Auto-generated method stub
		Set<K> keysetDisk= new HashSet<K>();
		
		if(internalSet !=null && internalSize > 0) {
			
			
			for (MapEntry<K, V> mapEntry : internalSet) {
				keysetDisk.add(mapEntry.getKey());
			}
			if(onDisk) {
				
				HashSet<MapEntry<K, V>> tempSet = readDiskData();
				for (MapEntry<K, V> mapEntry : tempSet) {
					
					keysetDisk.add(mapEntry.getKey());
					
				}
				
				
			}
			
		}
		
		return keysetDisk;
	}

	@Override
	public Collection<V> values() {
		// TODO Auto-generated method stub
		Collection<V> values = new HashSet<V>();
		
		if (size>0) {
			for (MapEntry<K, V> mapEntry : internalSet) {

				values.add(mapEntry.getValue());
			} 
			if(onDisk) {
				
				HashSet<MapEntry<K, V>> diskData= readDiskData();
				for (MapEntry<K, V> mapEntry : diskData) {
					
					values.add(mapEntry.getValue());
					
				}
				
			}
			
		}
		return  values;
	}

	
	
	
	public synchronized void writeEntryObject(HashSet<MapEntry<K, V>> diskData) {
		try {
			
		FileOutputStream f = new FileOutputStream(new File(filePath));
		ObjectOutputStream o = new ObjectOutputStream(f);
		o.writeObject(diskData);
		f.close();
		o.close();
		
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		
		
	}
	
	public synchronized HashSet<MapEntry<K, V>> readDiskData() {
		try {
		FileInputStream fi = new FileInputStream(new File(filePath));
		ObjectInputStream oi = new ObjectInputStream(fi);
		
			
			HashSet<MapEntry<K, V>> diskData=(HashSet<MapEntry<K, V>>)oi.readObject();
			return diskData;
			
			
		
		
		}catch (EOFException eof) {
			
			eof.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public void rearangeObjects() {
		
		
		if(onDisk) {
			
			HashSet<MapEntry<K, V>> diskData = readDiskData();
			
			for (MapEntry<K, V> mapEntry : diskData) {
				
				internalSet.add(mapEntry);
				internalSize++;
				diskData.remove(mapEntry);
				break;
			}
			
			if(diskData.isEmpty()) {
				
				onDisk=false;
				try {
					finalize();
				} catch (Throwable e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}else {
				
				writeEntryObject(diskData);
			}
			
			
		}
		
	}
	
	public void initializeDiskEntry(HashSet<MapEntry<K, V>> mapEntries) {
		try {
			
		FileOutputStream f = new FileOutputStream(new File(filePath));
		ObjectOutputStream o = new ObjectOutputStream(f);
		o.writeObject(mapEntries);
		f.close();
		o.close();
		onDisk=true;
		
		}catch (EOFException eof) {
			eof.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	@Override
	protected void finalize() throws Throwable {
		// TODO Auto-generated method stub
	
		File file = new File(filePath);
	
		if(file.delete()) {
			
			System.out.println("Disk Data Deleted");
		}
	
		
		
	}
	
}
