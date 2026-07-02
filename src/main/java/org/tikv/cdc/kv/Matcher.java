package org.tikv.cdc.kv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.model.MutableEventRow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Matcher {
    private static final Logger logger = LoggerFactory.getLogger(Matcher.class);

    private Map<MatchKey, MutableEventRow> unmatchedValue; // 缓存未匹配的预写事件
    private Map<MatchKey, MutableEventRow> unmatchedCommitValue; // 缓存未匹配的commit事件

    private List<MutableEventRow> cachedCommit; // 缓存提交事件
    private List<MutableEventRow> cachedRollback; // 缓存回滚事件

    public Matcher() {
        this.unmatchedValue = new HashMap<>();
        this.unmatchedCommitValue = new HashMap<>();
        this.cachedCommit = new ArrayList<>();
        this.cachedRollback = new ArrayList<>();
    }

    // 将预写事件放入 unmatchedValue 缓存中
    public void putPrewriteRow(MutableEventRow row) {
        MatchKey key = new MatchKey(row.getStartTs(), row.getKey());
        // 跳过 fake prewrite 事件
        if (unmatchedValue.containsKey(key) && row.getValue().isEmpty()) {
            return;
        }
        unmatchedValue.put(key, row);
    }

    // 将commit事件放入 unmatchedValue 缓存中
    public void putCommitRow(MutableEventRow row) {
        MatchKey key = new MatchKey(row.getStartTs(), row.getKey());
        // 跳过 fake commit 事件
        if (unmatchedCommitValue.containsKey(key) && row.getValue().isEmpty()) {
            return;
        }
        unmatchedCommitValue.put(key, row);
    }

    // 匹配提交事件和预写事件 Commit
    public MutableEventRow matchCommitRow(MutableEventRow row, boolean initialized) {
        MatchKey key = new MatchKey(row.getStartTs(), row.getKey());
        MutableEventRow value = unmatchedCommitValue.get(key);
        if (value != null) {
            // TiKV 可能发送空值的 fake commit 事件
            if (!initialized && value.getValue().isEmpty()) {
                return null;
            }
            value.setValue(row.getValue());
            value.setOldValue(row.getOldValue());
            unmatchedCommitValue.remove(key);
            return value;
        }
        return null;
    }

    // 匹配提交事件和预写事件
    public boolean matchRow(MutableEventRow row, boolean initialized) {
        MatchKey key = new MatchKey(row.getStartTs(), row.getKey());
        MutableEventRow value = unmatchedValue.get(key);
        if (value != null) {
            // TiKV 可能发送空值的 fake prewrite 事件
            if (!initialized && value.getValue().isEmpty()) {
                return false;
            }
            row.setValue(value.getValue());
            row.setOldValue(value.getOldValue());
            unmatchedValue.remove(key);
            return true;
        }
        return false;
    }

    // 缓存提交事件
    public void cacheCommitRow(MutableEventRow row) {
        cachedCommit.add(row);
    }

    // 匹配缓存的提交事件
    public List<MutableEventRow> matchCachedRow(boolean initialized) {
        if (!initialized) {
            throw new IllegalStateException("Must be initialized before matching cached rows");
        }

        List<MutableEventRow> matchedCommit = new ArrayList<>();
        for (MutableEventRow cacheEntry : cachedCommit) {
            boolean matched = matchRow(cacheEntry, true);
            if (!matched) {
                logger.info(
                        "Ignore commit event without prewrite, key: {}, startTs: {}",
                        cacheEntry.getKey(),
                        cacheEntry.getStartTs());
                continue;
            }
            matchedCommit.add(cacheEntry);
        }
        return matchedCommit;
    }

    // 回滚事件处理
    public void rollbackRow(MutableEventRow row) {
        MatchKey key = new MatchKey(row.getStartTs(), row.getKey());
        unmatchedValue.remove(key);
    }

    // 缓存回滚事件
    public void cacheRollbackRow(MutableEventRow row) {
        cachedRollback.add(row);
    }

    // 处理缓存的回滚事件
    public void matchCachedRollbackRow(boolean initialized) {
        if (!initialized) {
            throw new IllegalStateException(
                    "Must be initialized before matching cached rollback rows");
        }
        List<MutableEventRow> rollback = this.cachedRollback;
        this.cachedRollback = new ArrayList<>();
        for (MutableEventRow cacheEntry : rollback) {
            rollbackRow(cacheEntry);
        }
    }

    public Map<MatchKey, MutableEventRow> getUnmatchedValue() {
        return unmatchedValue;
    }
}
