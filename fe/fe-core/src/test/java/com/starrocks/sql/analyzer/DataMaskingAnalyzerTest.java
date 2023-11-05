// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
