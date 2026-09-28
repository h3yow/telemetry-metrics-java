package com.github.h3yow.telemetrymetricsjava.metrics.model;

import java.util.List;

/**
 * 分组查询结果：完成状态 + 可解释说明 + （成功时）有序的分组窗口聚合。
 *
 * <p>结果排序确定：先按窗口起点升序，同一窗口内按 {@code groupLabels} 的
 * key/value 字典序排序；相同输入多次查询的顺序与内容一致。
 * 超限时 {@link #status()} 不是 {@link GroupQueryStatus#OK}，且 {@link #results()}
 * 为空列表——整单失败，绝不静默截断或跨组合合并。</p>
 */
public record GroupQueryResult(GroupQueryStatus status, String detail,
                               List<GroupWindowAggregation> results) {

    public GroupQueryResult {
        results = results == null ? List.of() : List.copyOf(results);
        detail = detail == null ? "" : detail;
    }

    public boolean isComplete() {
        return status == GroupQueryStatus.OK;
    }

    public static GroupQueryResult ok(List<GroupWindowAggregation> results) {
        return new GroupQueryResult(GroupQueryStatus.OK,
                "returned " + (results == null ? 0 : results.size()) + " group-window rows",
                results);
    }

    public static GroupQueryResult rejected(GroupQueryStatus status, String detail) {
        return new GroupQueryResult(status, detail, List.of());
    }
}
