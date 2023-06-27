package com.zmj.flink.cdc.entity;


import com.google.gson.Gson;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

/**
 * @BelongsProject: data-platform-parent
 * @BelongsPackage: com.zmj.flink.cdc.entity
 * @Author: kaiyuanyang
 * @CreateTime: 2023-06-09  10:52
 * @Description: TODO
 * @Version: 1.0
 */
@Getter
@Setter
@Builder
public class SqlServerCdcSource implements Serializable {

    private String sourceSchema;

    private String sourceTable;

    private int objectId;

    private int sourceObjectId;

    private String indexColumnList;

    private String capturedColumnList;

    private List<TableDesc> ddl;

    private String dorisDdl;

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
