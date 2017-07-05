package com.luo.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by luohui on 17/7/4.
 * MappedRingBufferMemoryIndex是单例,基于skip list结构的内存索引数据
 */
public class MappedRingBufferMemoryIndex<K, V> {
    private MappedRingBuffer<K, V> ringBuffer;
    private int slotCount = 0;
    private int indexBufferSize = 0;
    private String indexBufferDir;
    private String indexBufferNamePrefix;
    private int indexSliceSize = 0;
    private List<MappedByteBuffer> indexRecordBufferList;//每个slot都有一个MappedByteBuffer存放全量索引
    private List<Integer> indexBufferIdx;//每个slot对应的索引的id？
    private ConcurrentSkipListMap<K, MappedRingBufferIndexData> indexSliceMap = new ConcurrentSkipListMap<>();

    public MappedRingBufferMemoryIndex(MappedRingBuffer<K, V> ringBuffer, int slotCount, int indexBufferSize, String indexBufferDir, String indexBufferNamePrefix, int indexSliceSize) {
        this.ringBuffer = ringBuffer;
        this.slotCount = slotCount;
        this.indexBufferSize = indexBufferSize;
        this.indexBufferDir = indexBufferDir;
        this.indexBufferNamePrefix = indexBufferNamePrefix;
        this.indexSliceSize = indexSliceSize;

        this.indexRecordBufferList = new ArrayList<>(this.slotCount);
        this.indexBufferIdx = new ArrayList<>(this.slotCount);
    }

    //基于跳表
    public K firstKey() {
        return indexSliceMap.firstKey();
    }

    public K lastKey() {
        return indexSliceMap.lastKey();
    }

    //追加一条索引记录，索引应该存什么？为什么每条数据都创建一个index？
    public void writeIndexData(short slotIdx, K key, MappedRingBufferIndexData index) {

    }


    /**
     * 有索引文件，直接根据文件构建索引；
     * 没有，创建索引文件，导入索引
     * true 表示导入索引成功
     *
     * @param slotIdx
     * @return
     */
    public boolean buildIndex(short slotIdx) throws IOException {
        //fileNamePattern : this.indexBufferNamePrefix + "index-%d.%d", slotIdx, index
        String fileNamePattern = this.indexBufferNamePrefix + "index-" + slotIdx + ".";
        File dir = new File(this.indexBufferDir);
        String slotFileName = null;
        //查找当前slot有效索引
        int index = 0;
        for (String name : dir.list()) {
            if (name.startsWith(fileNamePattern)) {
                int temp = Integer.valueOf(name.substring(name.lastIndexOf(".")));
                if (temp > index) {//难道包含了slotIdx的索引文件不是唯一的吗？
                    index = temp;
                    slotFileName = name;
                }
            }
        }

        //之前已经有索引文件
        if (slotFileName != null) {
            indexBufferIdx.add(slotIdx, index);//难道不是相等吗？一个slot会有多个index文件？
            slotFileName = this.getIndexBufferName(slotIdx);
            MappedByteBuffer indexByteBuffer = this.newByteBuffer(slotFileName, this.indexBufferSize);
            System.out.println("index buffer success " + slotFileName);
            indexRecordBufferList.add(slotIdx, indexByteBuffer);
            this.buildMemIndex(slotIdx);
            return true;
        } else {
            //之前没有索引文件，会从数据文件重新开始构建索引记录
            indexBufferIdx.add(slotIdx, 0);
            this.createIndexFile(slotIdx);
        }
        return false;
    }

    //创建索引文件
    private void createIndexFile(short slotIdx) throws IOException {
        MappedByteBuffer indexByteBuffer = this.newByteBuffer(this.getIndexBufferName(slotIdx), this.indexBufferSize);
        indexByteBuffer.putInt(0);//0表示索引记录数为0个
        this.indexRecordBufferList.add(slotIdx, indexByteBuffer);//当前slot使用的哪个index buffer
    }

    private MappedByteBuffer newByteBuffer(String indexBufferName, int indexBufferSize) throws IOException {
        File file = new File(indexBufferName);
        if (!file.exists()) file.createNewFile();
        RandomAccessFile raFile = null;
        FileChannel chanel = null;
        try {
            raFile = new RandomAccessFile(indexBufferName, "rw");
            chanel = raFile.getChannel();
            MappedByteBuffer indexByteBuffer = chanel.map(FileChannel.MapMode.READ_WRITE, 0, indexBufferSize);
            System.out.println("newByteBuffer success " + indexBufferName);
            return indexByteBuffer;
        } finally {
            chanel.close();
            raFile.close();
        }
    }

    private String getIndexBufferName(short slotIdx) {
        int index = indexBufferIdx.get(slotIdx);//一个slot多个index文件
        ++index;
        indexBufferIdx.set(slotIdx, index);
        return String.format(this.indexBufferDir + "/" + this.indexBufferNamePrefix + "index-%d.%d", slotIdx, index);
    }

    /**
     * 构建跳表索引
     * 规则：插入每个连续空间的第一个、最后一个
     *
     * @param slotIdx
     */
    private void buildMemIndex(short slotIdx) {
        MappedByteBuffer indexByteBuffer = this.indexRecordBufferList.get(slotIdx);
        int total = this.getIndexRecCount(indexByteBuffer);
        //表示没有数据
        if (0 == total) {
            indexByteBuffer.position(4);
            return;
        }
        //indexByteBuffer存所有数据，heap中只存每隔slice的数据
        for (int num = this.indexSliceSize; num < total; num += this.indexSliceSize) {
            put2MemIndex(indexByteBuffer, num);
        }

        //确保第一条数据也放到heap中
        if (total > 1) {
            put2MemIndex(indexByteBuffer, 1);
        }
        //确保第后一条数据也放到heap中
        put2MemIndex(indexByteBuffer, total);

        //移动position到最后的位置
        indexByteBuffer.position(4 + total * this.getIndexRecSize());
    }

    //挑出的数据，存到heap中，也就是indexSliceMap中
    private void put2MemIndex(MappedByteBuffer indexByteBuffer, int index) {
        K key = this.ringBuffer.getFactory().newRingBufferKey();//一个空的key，等待填充
        MappedRingBufferIndexData data = new MappedRingBufferIndexData();
        this.readFromSlice(indexByteBuffer, index, key, data);
        this.indexSliceMap.put(key, data);
    }

    /**
     * 从分片读？
     *
     * @param indexByteBuffer
     * @param from
     * @param key
     * @param data
     */
    private void readFromSlice(MappedByteBuffer indexByteBuffer, int from, K key, MappedRingBufferIndexData data) {
        //索引文件中有多少条数据
        int total = this.getIndexRecCount(indexByteBuffer);
        if (from > total || from < 1) {
            throw new IllegalArgumentException("from illegal " + from);
        }

        //=======填充key=========
        int fixLen = this.ringBuffer.getFactory().fixedKeyLength();
        byte[] keyBuf = new byte[fixLen];
        // index records count(4 bytes) | [key + slot index(2 bytes)+ position(4 bytes)] | ........
        int temp = 4 + this.getIndexRecSize() * (from - 1);//什么意思？计算key的位置？
        for (int i = 0; i < fixLen; i++) {
            byte b = indexByteBuffer.get(temp);
            temp += 1;
            keyBuf[i] = b;
        }
        this.ringBuffer.getFactory().decodeKey(keyBuf, key);//填充key
        //=======填充key=======

        //=======填充value======================
        //这里temp不需要先加个key length ?
        short slotIndex = indexByteBuffer.getShort(temp);
        temp += 2;

        int position = indexByteBuffer.getInt(temp);
        temp += 4;

        //index所在的
        int indexRecIdx = indexByteBuffer.getInt(temp);
        temp += 4;

        //
        data.setSlotIndex(slotIndex);
        data.setIndexRecIndex(indexRecIdx);
        data.setPosition(position);
        //=======填充value======================

        if (data.getIndexRecIndex() != from) {
            throw new IllegalStateException("from not equal indexRecIndex");
        }
    }

    /**
     * 索引文件中有多少条数据
     *
     * @param indexByteBuffer
     * @return
     */
    private int getIndexRecCount(MappedByteBuffer indexByteBuffer) {
        return indexByteBuffer.getInt(0);
    }

    //一条索引的长度
    public int getIndexRecSize() {
        //key length + slot index + position + index_rec_idx（该索引所在index的id）
        return this.ringBuffer.getFactory().fixedKeyLength() + 2 + 4 + 4;
    }

    public K getMaxKey(short slotIdx) {
        MappedByteBuffer indexByteBuffer = this.indexRecordBufferList.get(slotIdx);
        int total = this.getIndexRecCount(indexByteBuffer);
        if (0 == total) return null;
        K key = this.ringBuffer.getFactory().newRingBufferKey();//一个空的key，等待填充
        MappedRingBufferIndexData data = new MappedRingBufferIndexData();
        this.readFromSlice(indexByteBuffer, total, key, data);
        return key;
    }
}
