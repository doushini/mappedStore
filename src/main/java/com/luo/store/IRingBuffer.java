package com.luo.store;

/**
 * Created by luohui on 17/7/4.
 */
public interface IRingBuffer<K, V> {
    void put(K k, V v);

    V get(K k);

    byte[] getRaw(K key);

    V next(K from , K nextKey);

    V previous(K from , K previousKey);

    K firstKey();

    K lastKey();

    long size();

    void truncate();

    IRingBuffer<K,V> setFactory(IRingBufferFactory<K,V> factory);
}
