package com.luo.store;

import lombok.Getter;

/**
 * Created by luohui on 17/7/4.
 */
public abstract class AbstractRingBuffer<K,V> implements IRingBuffer<K,V> {
    @Getter
    private IRingBufferFactory<K, V> factory;

    @Override
    public void put(K k, V v) {
        //如果写入的key小于最后的key，则需要覆盖
        K lastKey = this.lastKey();
        if(null == lastKey || ((Comparable)k).compareTo(lastKey)>0){
            this.writeData(k,v);
        }else{
            this.overrideData(k,v);
        }
    }

    abstract void writeData(K k, V v);

    abstract void overrideData(K k, V v);

    @Override
    public V get(K k) {
        return null;
    }

    @Override
    public byte[] getRaw(K key) {
        return new byte[0];
    }

    @Override
    public V next(K from, K nextKey) {
        return null;
    }

    @Override
    public V previous(K from, K previousKey) {
        return null;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public void truncate() {

    }

    @Override
    public IRingBuffer<K, V> setFactory(IRingBufferFactory<K, V> factory) {
        this.factory = factory;
        return this;
    }



}
