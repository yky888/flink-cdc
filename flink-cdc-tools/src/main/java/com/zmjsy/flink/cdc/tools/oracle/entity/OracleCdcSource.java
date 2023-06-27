package com.zmjsy.flink.cdc.tools.oracle.entity;


import com.google.gson.Gson;
import com.zmjsy.flink.cdc.tools.sqlserver.entity.TableDesc;
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
public class OracleCdcSource implements Serializable {

    private String sourceSchema;

    private String sourceTable;

    private String tableDesc;

    private List<TableDesc> ddl;

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
