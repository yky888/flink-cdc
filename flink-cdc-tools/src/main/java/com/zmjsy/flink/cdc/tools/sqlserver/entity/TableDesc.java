package com.zmjsy.flink.cdc.tools.sqlserver.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * @BelongsProject: data-platform-parent
 * @BelongsPackage: com.zmj.flink.cdc.entity
 * @Author: kaiyuanyang
 * @CreateTime: 2023-06-09  11:48
 * @Description: TODO
 * @Version: 1.0
 */
@Getter
@Setter
@Builder
public class TableDesc implements Serializable {

    private String tableName;

    private String tableComment;

    private String columnName;

    private String columnType;

    private String columnComment;

    //0 否 1是
    private String isNull;

    // 0否 1是
    private String isUniqueKey;

}
