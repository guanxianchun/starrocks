package com.starrocks.sql.analyzer;

import com.starrocks.sql.analyzer.masking.DataMaskingAnalyzer;
import com.starrocks.sql.ast.QueryStatement;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @ClassName DataMaskingAnalyzerTest
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/5 下午1:02
 */
public class DataMaskingAnalyzerTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    public void analyzeSql(String sql) {
        new DataMaskingAnalyzer().analyzeStatement((QueryStatement) AnalyzeTestUtil.analyzeSuccess(sql));
    }
    @Test
    public void testSimpleSingQuery() {
        String sql = "select t1.v1 as v1, t1.v2 as v2 , t1.v3 from t0 as t1";
        analyzeSql(sql);
    }
}
