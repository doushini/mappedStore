package com.luo.store;

import lombok.Data;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 * Created by luohui on 17/7/4.
 * Slot数据结构
 * / last position(4 bytes) | key length(1 byte) | key (N bytes) | data length(4 bytes) | data(N bytes) | checkSum(4 bytes) | ......
 * last position每次写数据均更新，帮助从文件恢复时准确定位Buffer的position
 * key length是固定的
 * <p>
 * 索引分2部分（1、heap中包含部分索引数据，2、direct buffer中包含全部的索引数据）
 * 每个slot对应一个索引index（全部的索引数据都在direct buffer中）
 * 每个slot的第一个和最后一个记录的索引记录必须放在heap中
 * 同一个slot中每间隔100条记录，则把索引记录放到heap中；
 * heap中相邻的两个索引记录形成一个slice，而slice中所有的数据存放于direct buffer中;
 * 切换slot，删除整个slot的所有heap和redict中的索引记录;
 * 回写记录：删除本slot尾部的部分索引记录（包含heap和direct）
 * 读：先从heap中定位目标slice，然后到direct buffer中查询索引记录
 * <p>
 * <p>
 * Index数据结构
 * / index records count(4 bytes) | [key + slot index(2 bytes)+ position(4 bytes)] | ........
 */
@Data
public class MappedRingBufferSlot<K, V> {
    private String fileDir;
    private String fileName;
    private long fileSize;
    private short slotIdx;
    private MappedByteBuffer dataByteBuffer = null;
    private final static int DATA_INIT_POS_IN_BUFFER = 4;
    private RandomAccessFile raFile = null;
    private FileChannel fc = null;
    private FileLock fl = null;
    private MappedRingBufferMemoryIndex<K, V> memoryIndex;
    private MappedRingBuffer<K, V> ringBuffer;
    private K maxKey = null;

    public MappedRingBufferSlot(String fileDir, String fileName, long fileSize, short slotIdx, MappedRingBufferMemoryIndex<K, V> memoryIndex, MappedRingBuffer<K, V> ringBuffer) {
        this.fileDir = fileDir;
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.slotIdx = slotIdx;
        this.memoryIndex = memoryIndex;
        this.ringBuffer = ringBuffer;
    }

    //一条数据占用字节数：1(key length) + 4(data length) + 16(key) + 1000(data) + 4(checksum) = 1025
    public int nextDataPosition(int length0, int length1) {
        int total = 1 + 4 + length0 + length1 + 4;
        int oldPosition = this.dataByteBuffer.position();
        int newPosition = oldPosition + total;
        if (newPosition > this.dataByteBuffer.limit()) {
            return -1;
        }
        this.dataByteBuffer.position(newPosition);
        this.dataByteBuffer.putInt(0, newPosition);
        return oldPosition;//why?
    }

    public void clean() {
        this.dataByteBuffer.clear();
        this.dataByteBuffer.putInt(DATA_INIT_POS_IN_BUFFER);//缺省的newPosition，第一个就是4
//        this.memoryIndex.removeSlotIndex(this.slotIdx,this.maxKey);
        this.maxKey = null;
    }

    //一条数据占用字节数：1(key length) + 4(data length) + 16(key) + 1000(data) + 4(checksum) = 1025
    public void writeData(int oldPosition, K key, byte[] keyBuffer, byte[] dataBuffer) {
        int temp = oldPosition;
        this.dataByteBuffer.put(temp, (byte) keyBuffer.length);
        temp++;

        this.dataByteBuffer.put(keyBuffer);
        temp += keyBuffer.length;

        this.dataByteBuffer.putInt(temp, dataBuffer.length);
        temp += 4;

        this.dataByteBuffer.put(dataBuffer);
        temp += dataBuffer.length;

        java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();
        crc32.update(dataBuffer, 0, dataBuffer.length);
        int checkSum = (int) crc32.getValue();
        this.dataByteBuffer.putInt(temp, checkSum);
        //end

        this.maxKey = key;//maxKey应该是currentKey
        //begin write index
        MappedRingBufferIndexData index = new MappedRingBufferIndexData(this.getSlotIdx(), oldPosition);
        this.memoryIndex.writeIndexData(slotIdx, key, index);
    }

    public K load() {
        try {
            this.loadData();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return this.maxKey;
    }

    private void loadData() throws IOException {
        File file = new File(this.fileDir + "/" + this.fileName);
        boolean createNewFile = false;
        if (!file.exists()) {
            System.out.println("slot file [" + file.getName() + "] is missing");
            file.createNewFile();
            createNewFile = true;
        }

        raFile = new RandomAccessFile(file, "rw");
        fc = raFile.getChannel();
        fl = fc.lock();
        if (fl == null) {
            System.out.println("");
            throw new IllegalStateException("file is used by other process !");
        }
        if (fc.size() > 0 && fc.size() > this.fileSize) {
            throw new IllegalStateException("file size is larger than confiured");
        }

        long capacity = fc.size() > this.fileSize ? fc.size() : this.fileSize;
        this.dataByteBuffer = fc.map(FileChannel.MapMode.READ_WRITE, 0, capacity);
        if (createNewFile) {
            this.dataByteBuffer.putInt(DATA_INIT_POS_IN_BUFFER);
        } else {
            int lastPos = this.dataByteBuffer.getInt(0);
            this.dataByteBuffer.position(lastPos);
        }

        //构建索引
        this.buildIndex();
        System.out.println("load file end ");
    }

    private void buildIndex() throws IOException {
        if(!this.tryBuildIndexFromIndexFile()){
            this.buildIndexFromDataFile();
        }
    }

    /**
     * 从数据文件建立索引和跳表数据
     *
     */
    private void buildIndexFromDataFile() {
        int lastPos = this.dataByteBuffer.getInt(0);
        int position = 4;
        int count = 0;
        K firstKey = null;
        K lastKey = null;
        while (position<lastPos){
            int beginPos = position;
            //填充key
            int keyLen = this.dataByteBuffer.get(position);
            position+=1;
            byte[] keyBuf = new byte[keyLen];
            for (int n = 0; n < keyLen; n++) {
                byte b = this.getDataByteBuffer().get(position);
                position+=1;
                keyBuf[n]=b;
            }
            K key = this.ringBuffer.getFactory().newRingBufferKey();
            this.ringBuffer.getFactory().decodeKey(keyBuf,key);
            if(null == firstKey) firstKey = key;
            lastKey = key;

            //填充data
            int dataLen = this.dataByteBuffer.getInt(position);
            position+=4;
            position+=dataLen;

            //checksum
            position+=4;
            MappedRingBufferIndexData data = new MappedRingBufferIndexData(slotIdx,beginPos);
            this.maxKey = key;
            this.memoryIndex.writeIndexData(slotIdx,key, data);
            ++count;
            if(position>=lastPos){
                break;
            }
        }
        System.out.println("buildIndex, slotIndex "+slotIdx+" count "+count+" firstKey "+firstKey+" lastKey "+lastKey);
        this.dataByteBuffer.position(lastPos);
    }

    //根据上次的索引文件构建跳表数据
    private boolean tryBuildIndexFromIndexFile() throws IOException {
        if(memoryIndex.buildIndex(slotIdx)){
            this.maxKey = this.memoryIndex.getMaxKey(this.slotIdx);
            return true;
        }
        return false;
    }
}
