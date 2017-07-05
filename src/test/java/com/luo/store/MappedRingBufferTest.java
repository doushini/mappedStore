package com.luo.store;

import com.google.common.base.Preconditions;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * Created by luohui on 17/7/4.
 */
public class MappedRingBufferTest {
    private MappedRingBuffer<RingBufferKey, RingBufferData> mappedRingBuffer;
    private IRingBufferFactory<RingBufferKey, RingBufferData> factory;

    @Before
    public void before() {
        mappedRingBuffer = new MappedRingBuffer<>();
        factory = new IRingBufferFactory<RingBufferKey, RingBufferData>() {
            @Override
            public RingBufferKey newRingBufferKey() {
                return new RingBufferKey();
            }

            @Override
            public RingBufferData newRingBufferData() {
                return new RingBufferData(0);
            }

            @Override
            public int fixedKeyLength() {
                return 16;
            }

            @Override
            public byte[] encodeKey(RingBufferKey key) {
                ByteBuffer byteBuffer = ByteBuffer.allocate(this.fixedKeyLength());
                byteBuffer.putInt(key.changeId);
                byteBuffer.putInt(key.logIndex);
                byteBuffer.putLong(key.logPosition);
                return byteBuffer.array();
            }

            @Override
            public byte[] encodeData(RingBufferData data) throws UnsupportedEncodingException {
                return RingBufferData.ringBufferEncode(data);
            }

            @Override
            public void decodeKey(byte[] bytes, RingBufferKey key) {
                ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
                key.setChangeId(byteBuffer.getInt());
                key.setLogIndex(byteBuffer.getInt());
                key.setLogPosition(byteBuffer.getLong());
            }

            @Override
            public void decodeData(byte[] bytes, RingBufferData data) throws UnsupportedEncodingException {
                RingBufferData.ringBufferDecode(bytes, data);
            }
        };
        long start = System.currentTimeMillis();
        this.initialize();
        long end = System.currentTimeMillis();
        System.out.println("load " + (end - start));
    }

    private void initialize() {
        mappedRingBuffer.setFactory(factory);
        mappedRingBuffer.setDataFileCount((short)10);
        mappedRingBuffer.setDataFileDir("ringBufferData");
        mappedRingBuffer.setDataFileNamePrefix("ringBufferData");
        mappedRingBuffer.setDataFileSize(100*1024*1024L);//100M
        mappedRingBuffer.load();
    }

    @Test
    public void test() {
        int total = 10000;
        for (int n = 1; n <= total; n++) {
            int num = n * 2;
            //key 占用16字节长度
            RingBufferKey key = new RingBufferKey(num, num, num);
            //data 占用1000个字节
            RingBufferData data = new RingBufferData(num);
            //一条数据占用字节数：1(key length) + 4(data length) + 16(key) + 1000(data) + 4(checksum) = 1025
            this.mappedRingBuffer.put(key, data);
            //一个ringBuffer的Node（总共有40个node）可以存放(1000 * 1000 -4)/1025=975.60条记录，4是在putInt(0,newPosition)
        }

        RingBufferKey firstKey = mappedRingBuffer.firstKey();
        Preconditions.checkArgument(firstKey.equals(new RingBufferKey(2,2,2)));

        RingBufferKey lastKey = mappedRingBuffer.lastKey();
        Preconditions.checkArgument(lastKey.equals(new RingBufferKey(2*total,2*total,2*total)));

        int slotPos0 = mappedRingBuffer.getSlotPosition((short)0);
        int slotPos1 = mappedRingBuffer.getSlotPosition((short)1);
        Preconditions.checkArgument(slotPos0 ==(4 + 975*1025));
        Preconditions.checkArgument(slotPos1 ==(4 + 975*1025));
    }
}
