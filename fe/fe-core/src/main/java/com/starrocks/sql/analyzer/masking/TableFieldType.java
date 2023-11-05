package com.starrocks.sql.analyzer.masking;

/**
 * @ClassName TableNameType
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/5 上午11:59
 */
public enum TableFieldType {
    CATALOG,
    CATALOG_DB,
    CATALOG_DB_TBL,
    DB, DB_TBL,
    TBL,
    TBL_COLUMN,
    TBL_COLUMN_ALIAS
}
