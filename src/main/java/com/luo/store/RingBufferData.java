package com.luo.store;

import java.io.UnsupportedEncodingException;

/**
 * Created by luohui on 17/7/4.
 */
public class RingBufferData {
    private String data;

    //for test
    public RingBufferData(int n) {
        StringBuffer sb = new StringBuffer(1000);
        sb.append(n);
        int num = 1000;
        while (--num > 0) {
            sb.append("A");
        }
        data = sb.toString();
    }

    public static byte[] ringBufferEncode(RingBufferData data) throws UnsupportedEncodingException {
        return data.data.getBytes("utf-8");
    }

    public static void ringBufferDecode(byte[] bytes, RingBufferData data) throws UnsupportedEncodingException {
        data.data = new String(bytes,"utf-8");
    }
}
