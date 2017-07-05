package com.luo.store;

import java.io.UnsupportedEncodingException;

/**
 * Created by luohui on 17/7/4.
 */
public interface IRingBufferFactory<K, V> {
    K newRingBufferKey();

    V newRingBufferData();

    int fixedKeyLength();

    byte[] encodeKey(K key);

    byte[] encodeData(V data) throws UnsupportedEncodingException;

    void decodeKey(byte[] bytes, K key);

    void decodeData(byte[] bytes, V data) throws UnsupportedEncodingException;
}
