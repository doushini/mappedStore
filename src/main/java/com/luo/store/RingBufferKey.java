package com.luo.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by luohui on 17/7/4.
 * 16个字节
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RingBufferKey implements Comparable<RingBufferKey> {
    public int changeId;
    public int logIndex;
    public long logPosition;

    @Override
    public int compareTo(RingBufferKey other) {
        if (this.getChangeId() != other.getChangeId())
            return this.getChangeId() - other.getChangeId();
        if (this.getLogIndex() != other.getLogIndex())
            return this.getLogIndex() - other.getLogIndex();
        if (this.getLogPosition() == other.getLogPosition())
            return 0;
        return this.getLogPosition() > other.getLogPosition() ? 1 : -1;
    }
}
