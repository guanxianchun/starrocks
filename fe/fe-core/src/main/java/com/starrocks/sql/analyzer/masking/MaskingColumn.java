package com.starrocks.sql.analyzer.masking;

import java.util.Objects;

/**
 * @ClassName PhysicalMaskingColumn
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/7 下午10:10
 */
public class MaskingColumn {
    private String database;
    private String tableName;
    private String name;

    public MaskingColumn(String database, String tableName, String name) {
        this.database = database;
        this.tableName = tableName;
        this.name = name;
    }

    public String getDatabase() {
        return database;
    }

    public MaskingColumn setDatabase(String database) {
        this.database = database;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public MaskingColumn setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getName() {
        return name;
    }

    public MaskingColumn setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MaskingColumn that = (MaskingColumn) o;
        return Objects.equals(database, that.database) && Objects.equals(tableName, that.tableName) && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, tableName, name);
    }
}
