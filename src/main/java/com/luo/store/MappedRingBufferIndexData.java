package com.luo.store;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by luohui on 17/7/5.
 */
@Data
@NoArgsConstructor
public class MappedRingBufferIndexData {
    private short slotIndex;//数据所在的slot
    private int position;//数据在slot中的位置
    private int indexRecIndex;//所在的索引

    public MappedRingBufferIndexData(short slotIndex, int position) {
        this.slotIndex = slotIndex;
        this.position = position;
    }
}
