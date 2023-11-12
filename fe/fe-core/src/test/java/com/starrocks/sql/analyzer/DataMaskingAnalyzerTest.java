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

import com.google.gson.annotations.SerializedName;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.masking.DataMaskingAnalyzer;
import com.starrocks.sql.ast.CreateColumnPolicyStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @ClassName DataMaskingAnalyzerTest
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/5 下午1:02
 */
public class DataMaskingAnalyzerTest {
    private static final String ENGINE = "ENGINE=OLAP\n";
    private static final String DUPLICATE_KEYS_FORMAT = "DUPLICATE KEY(%s) \n";
    private static final String DISTRIBUTED_FORMAT = "DISTRIBUTED BY HASH(%s) BUCKETS 3 \n";
    private static final String PROPERTIES = "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\"\n" +
            ")";

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        initTable();
    }

    private static void initTable() throws Exception {
        String sql = "create table students (id int , name varchar(32), age int,  height int) \n "
                + ENGINE + String.format(DUPLICATE_KEYS_FORMAT, "id")
                + String.format(DISTRIBUTED_FORMAT, "id")
                + PROPERTIES;
        createTable(sql);

        sql = "create table course (id int , name varchar(32)) \n "
                + ENGINE + String.format(DUPLICATE_KEYS_FORMAT, "id")
                + String.format(DISTRIBUTED_FORMAT, "id")
                + PROPERTIES;
        createTable(sql);


        sql = "create table student_course (id int , student_id int, course_id int, score int) \n "
                + ENGINE + String.format(DUPLICATE_KEYS_FORMAT, "id")
                + String.format(DISTRIBUTED_FORMAT, "id")
                + PROPERTIES;
        createTable(sql);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                AnalyzeTestUtil.getConnectContext());
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
    }

    private static void createPolicy(String sql) throws Exception {
        CreateColumnPolicyStmt createTableStmt = (CreateColumnPolicyStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                AnalyzeTestUtil.getConnectContext());
        //GlobalStateMgr.getCurrentState().createTable(createTableStmt);
    }

    public void analyzeSql(String sql) {
        new DataMaskingAnalyzer().analyzeStatement((QueryStatement) AnalyzeTestUtil.analyzeSuccess(sql));
    }

    @Test
    public void testSimpleSingQuery() {
        String sql = "select t1.id as student_id, t1.name as student_name , t1.age, t1.height from students as t1";
        analyzeSql(sql);
    }

    @Test
    public void testCaseWhenQuery() {
        String sql = "select t1.id as student_id, t1.name as student_name, \n" +
                "  case when t1.age < 12 then '小学生' \n" +
                "       when t1.age < 15 then '初中生' \n" +
                "       else '高中生' end student_type_name \n" +
                " from students as t1";
        analyzeSql(sql);
    }

    @Test
    public void testFunctionNestQuery() {
        String sql = "select t1.id as student_id, t1.name as student_name, \n" +
                "  abs(abs(t1.height+10)) as student_height, \n" +
                "  abs(abs(t1.height+t1.age)) as student_height_add_age , \n" +
                "  case when t1.age < 12 then '小学生' \n" +
                "       when t1.age < 15 then '初中生' \n" +
                "       else '高中生' end student_type_name \n" +
                " from students as t1";
        analyzeSql(sql);
    }

    @Test
    public void testMultiJoinQuery() {
        String sql = "select t1.id as student_id, t1.name as student_name, \n" +
                "  t3.name as course_name,  \n" +
                "  t2.score as score \n" +
                " from students as t1, student_course as t2, course as t3  \n" +
                " where t1.id = t2.student_id and t3.id = t2.course_id ";
        analyzeSql(sql);
    }

    @Test
    public void testCTEQuery() {
        String sql = "with tt as (select t1.id as student_id, t1.name as student_name, \n" +
                "  t3.name as course_name,  \n" +
                "  t2.score as score \n" +
                " from students as t1, student_course as t2, course as t3  \n" +
                " where t1.id = t2.student_id and t3.id = t2.course_id) \n" +
                " select tmp.student_id , tmp.student_name, tmp.course_name, tmp.score from (select * from tt) as tmp";
        analyzeSql(sql);
    }

    @Test
    public void testNestQuery() {
        String sql = "select tmp.student_id , tmp.student_name, tmp.course_name, tmp.score \n" +
                "from  (select t1.id as student_id, t1.name as student_name, \n" +
                "  t3.name as course_name,  \n" +
                "  t2.score as score \n" +
                " from students as t1, student_course as t2, course as t3  \n" +
                " where t1.id = t2.student_id and t3.id = t2.course_id) as tmp \n";
        analyzeSql(sql);
    }

    @Test
    public void testIfQuery() {
        String sql = "select t1.id as student_id, " +
                "  t1.name as student_name, \n" +
                "  abs(abs(t1.height+10)) as student_height, \n" +
                "  abs(abs(t1.height+t1.age)) as student_height_add_age , \n" +
                "  if(t1.age < 12, '小学生', t1.name ) as child_student \n" +
                " from students as t1";
        analyzeSql(sql);
        Person student = new Student("hello", 29);
        String json = GsonUtils.GSON.toJson(student);
        Person person = GsonUtils.GSON.fromJson(json, Student.class);
        System.out.println(person);
    }

    public class Person {
        @SerializedName("name")
        private String name;

        public Person(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public Person setName(String name) {
            this.name = name;
            return this;
        }
    }

    public class Student extends Person {
        @SerializedName("age")
        private int age = 30;

        public Student(String name, int age) {
            super(name);
            this.age = age;
        }

        public int getAge() {
            return age;
        }

        public Student setAge(int age) {
            this.age = age;
            return this;
        }
    }

    @Test
    public void testCreateColumnPolicy() throws Exception {
        String sql = "create column policy users_policy on students when (current_user = 'jack') add column age with abs(age);";
        createPolicy(sql);
    }
}
