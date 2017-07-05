package com.luo.store;

import lombok.Data;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by luohui on 17/7/4.
 * use java mmap to persistence data,also is a ringBuffer with 40 slot which is a a direct byteBuffer
 */
@Data
public class MappedRingBuffer<K, V> extends AbstractRingBuffer<K, V> {
    private String dataFileDir = "./data";
    private String dataFileNamePrefix = "data-ringBuffer-";
    private short dataFileCount = 40;//等于slotCount
    private long dataFileSize = 1073741824L;
    private List<MappedRingBufferSlot<K, V>> slotList = new ArrayList<>();
    private int currentSlotIdx = 0;
    private MappedRingBufferMemoryIndex<K, V> memoryIndexer;
    private int indexSliceSize = 300;

    @Override
    void writeData(K key, V data) {
        MappedRingBufferSlot<K, V> currentSlot = this.getCurrentSlot();
        byte[] keyBuffer = this.getFactory().encodeKey(key);
        byte[] dataBuffer;
        try {
            dataBuffer = this.getFactory().encodeData(data);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }

        int oldPosition = currentSlot.nextDataPosition(keyBuffer.length, dataBuffer.length);
        //current slot buffer is full, switch to next
        if (-1 == oldPosition) {
            currentSlot = this.switchNextSlot();
            oldPosition = currentSlot.nextDataPosition(keyBuffer.length, dataBuffer.length);
        }
        currentSlot.writeData(oldPosition, key, keyBuffer, dataBuffer);
    }

    private MappedRingBufferSlot<K, V> switchNextSlot() {
        MappedRingBufferSlot<K, V> slot = slotList.get(++currentSlotIdx);
        slot.clean();
        return slot;
    }

    @Override
    void overrideData(K k, V v) {

    }

    public void load() {
        this.checkLoad();
        this.memoryIndexer = new MappedRingBufferMemoryIndex<K, V>(this, dataFileCount, (int) (dataFileSize / 5), dataFileDir, dataFileNamePrefix, indexSliceSize);
        K tempKey = this.getFactory().newRingBufferKey();
        for (short fileNo = 0; fileNo < dataFileCount; fileNo++) {
            MappedRingBufferSlot<K, V> slot = new MappedRingBufferSlot<K, V>(dataFileDir, dataFileNamePrefix + fileNo, dataFileSize, fileNo, memoryIndexer, this);
            K lastKey = slot.load();
            System.out.println("load slot" + fileNo + " ,lastKey " + lastKey);
            slotList.add(slot);
            //寻找上次运行最后使用的slot
            if (null != lastKey && ((Comparable) lastKey).compareTo(tempKey) > 0) {
                this.currentSlotIdx = fileNo;//一直会覆盖
                tempKey = lastKey;//一直会覆盖
            }
        }
        System.out.println("load all slot complete! current slotIndex " + this.currentSlotIdx + " ,lastKey " + tempKey);
    }

    private void checkLoad() {
        File dir = new File(dataFileDir);
        if (!dir.exists()) dir.mkdirs();
    }

    public MappedRingBufferSlot<K, V> getCurrentSlot() {
        return this.slotList.get(this.currentSlotIdx);
    }


    int getSlotPosition(int slotIdx) {
        return this.slotList.get(slotIdx).getDataByteBuffer().getInt(0);//为什么不是getInt?
    }

    @Override
    public K firstKey() {
        try {
            return this.memoryIndexer.firstKey();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public K lastKey() {
        try {
            return this.memoryIndexer.lastKey();
        } catch (NoSuchElementException e) {
            return null;
        }
    }
}
