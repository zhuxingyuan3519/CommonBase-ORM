using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;
using FastReflectionLib;
using System.Xml;

namespace DBUtility
{
    public class CommonBase
    {
        public static DbHelperSQL helpSQL = new DbHelperSQL();
        public CommonBase(string ConnectionString = "")
        {
            if(!string.IsNullOrEmpty(ConnectionString))
            {
                helpSQL = new DbHelperSQL(ConnectionString);
            }
        }

        #region DataTable方法
        private static List<string> SqlKey = new List<string>
        {
            "select",",","，","--","－","order","delete","update","insert","drop","table","create","declare","exec"
        };
        /// <summary>
        /// 执行事物
        /// </summary>
        /// <param name="SQLStringList"></param>
        /// <returns></returns>
        public static bool RunHashtable(Hashtable hashtable)
        {
            if(DbHelperSQL.ExecuteSqlTranWithIndentity(hashtable) != -1)
                return true;
            return false;
        }

        public static bool RunListCommit(List<CommonObject> commandInfoList)
        {
            if (commandInfoList.Count == 0)
                return true;
            List<CommandInfo> listSql = new List<CommandInfo>();
            foreach(CommonObject sqlStr in commandInfoList)
            {
                listSql.Add(new CommandInfo(sqlStr.CommandText, sqlStr.parameters));
            }
            //顺序执行SQL
            return DbHelperSQL.ExecuteSqlTranWithIndentity(listSql);
        }

        public static bool TestSql(string mkey)
        {
            //foreach (string str in SqlKey)
            //{
            //    if (mkey.ToUpper().Contains(str.ToUpper()) || mkey.ToUpper().Contains(str.ToUpper()))
            //        return false;
            //}
            return true;
        }

        public static DataTable GetTable(string tableName, string primaryKey, string orderBy, string strWhere, int pageIndex, int pageSize, out int count)
        {
            SqlParameter[] parameters = {
                    new SqlParameter("@TableName", SqlDbType.VarChar, 2000),
                    new SqlParameter("@FieldList", SqlDbType.VarChar, 50),
                    new SqlParameter("@PrimaryKey", SqlDbType.VarChar, 50),
                    new SqlParameter("@Where", SqlDbType.VarChar, 2000),
                    new SqlParameter("@Order", SqlDbType.VarChar, 50),
                    new SqlParameter("@SortType", SqlDbType.Int,4),
                    new SqlParameter("@RecorderCount", SqlDbType.Int,4),
                    new SqlParameter("@PageSize", SqlDbType.Int,4),
                    new SqlParameter("@PageIndex", SqlDbType.Int,4),
                    new SqlParameter("@TotalCount", SqlDbType.Int,4),
                    new SqlParameter("@TotalPageCount", SqlDbType.Int,4)};
            parameters[0].Value = tableName;
            parameters[1].Value = "TMP.*";
            parameters[2].Value = primaryKey;
            parameters[3].Value = strWhere;
            parameters[4].Value = orderBy;
            parameters[5].Value = 3;
            parameters[6].Value = 0;
            parameters[7].Value = pageSize;
            parameters[8].Value = pageIndex;
            parameters[9].Direction = ParameterDirection.InputOutput;
            parameters[10].Direction = ParameterDirection.InputOutput;

            DataTable table = DbHelperSQL.RunProcedure("P_AspNetPage", parameters, "txTable").Tables[0];
            count = Convert.ToInt32(parameters[9].Value);
            return table;
        }

        public static void RunProcedureNoReturn(string produceName, SqlParameter[] parameters)
        {
            DbHelperSQL.RunProcedureNoReturn("Proc_RecountBanlnceMoney", parameters);
        }

        public static DataTable GetProduceTable(SqlParameter[] parameters, string produceName)
        {
            DataTable table = new DataTable();
            DataSet ds = DbHelperSQL.RunProcedure(produceName, parameters, "txTable");
            if(ds != null && ds.Tables.Count > 0)
            {
                table = ds.Tables[0];
            }
            return table;
        }

        /// <summary>
        /// 另外一种分页方式，直接拼接SQL查询，没用存储过程
        /// </summary>
        /// <param name="CurrentPageIndex">当前页码</param>
        /// <param name="PageSize">分页条数，一页中多少行</param>
        /// <param name="strWhere">查询条件</param>
        /// <param name="filedName">要查询的字段名称</param>
        /// <param name="orderBy">排序</param>
        /// <param name="TableName">表名</param>
        /// <returns></returns>
        public static DataSet GetPageList(int CurrentPageIndex, int PageSize, string strWhere, string filedName, string orderBy, string TableName)
        {
            //查询出总数
            string strQuerySql = "SELECT COUNT(1) FROM " + TableName + " where " + strWhere;
            PageSize = PageSize <= 0 ? 10 : PageSize;
            if(CurrentPageIndex == 1) //如果是查询首页，就简单了,一句话
            {
                filedName = string.IsNullOrEmpty(filedName) ? "*" : filedName;
                if(string.IsNullOrEmpty(strWhere))
                    strQuerySql = "SELECT TOP  " + PageSize.ToString() + " " + filedName + ", ROW_NUMBER() OVER (ORDER BY " + orderBy + ") AS RowNumber FROM " + TableName + " ORDER BY " + orderBy + ";";
                else
                    strQuerySql = "SELECT TOP  " + PageSize.ToString() + " " + filedName + ", ROW_NUMBER() OVER (ORDER BY " + orderBy + ") AS RowNumber FROM " + TableName + " WHERE  " + strWhere + " ORDER BY " + orderBy + ";";
            }
            else //如果不是查询首页
            {
                string starRowNum = "10", endRowNum = "20";
                starRowNum = ((CurrentPageIndex - 1) * PageSize).ToString();
                endRowNum = (CurrentPageIndex * PageSize).ToString();
                if(string.IsNullOrEmpty(strWhere))
                    strQuerySql = "SELECT * FROM (SELECT " + filedName + ",ROW_NUMBER() OVER (ORDER BY " + orderBy + ") AS RowNumber FROM " + TableName + ") tmp";
                else
                    strQuerySql = "SELECT * FROM (SELECT " + filedName + ",ROW_NUMBER() OVER (ORDER BY " + orderBy + ") AS RowNumber FROM " + TableName + " WHERE  " + strWhere + ") tmp";
                strQuerySql += " WHERE RowNumber > " + starRowNum + " AND RowNumber <= " + endRowNum;
                //strQuerySql += " ORDER BY " + orderBy + ";";
            }
            string sqlCountSql = ";SELECT COUNT(1) Total FROM " + TableName + " where " + strWhere;
            DataSet ds = GetDataSet(strQuerySql + sqlCountSql);
            return ds;
        }

        public static DataTable GetPageDataTable(int CurrentPageIndex, int PageSize, string strWhere, string filedName, string orderBy, string TableName, out int totalCount)
        {
            SqlParameter[] parameters = {
                    new SqlParameter("@CurrentPageIndex", SqlDbType.Int, 4),
                    new SqlParameter("@PageSize", SqlDbType.Int, 4),
                    new SqlParameter("@strWhere", SqlDbType.VarChar, 2000),
                    new SqlParameter("@filedName", SqlDbType.VarChar, 2000),
                    new SqlParameter("@orderBy", SqlDbType.VarChar, 500),
                    new SqlParameter("@TableName", SqlDbType.VarChar,500),
                    new SqlParameter("@TotalCount", SqlDbType.Int,4)};
            parameters[0].Value = CurrentPageIndex;
            parameters[1].Value = PageSize;
            parameters[2].Value = strWhere;
            parameters[3].Value = filedName;
            parameters[4].Value = orderBy;
            parameters[5].Value = TableName;
            parameters[6].Direction = ParameterDirection.InputOutput;
            DataTable table = DbHelperSQL.RunProcedure("P_AspNetLayerPager", parameters, "txTable").Tables[0];
            totalCount = Convert.ToInt32(parameters[6].Value);
            return table;
        }

        public static List<T> GetPageList<T>(int CurrentPageIndex, int PageSize, string strWhere, string filedName, string orderBy, out int totalCount) where T : new()
        {
            List<T> list = new List<T>();
            Type type = typeof(T);
            PropertyInfo[] propertys = type.GetProperties();
            DataTable table = GetPageDataTable(CurrentPageIndex, PageSize, strWhere, filedName, orderBy, GetTableName(type), out totalCount);
            foreach(DataRow de in table.Rows)
            {
                T t = new T();
                t = TranEntity<T>(propertys, de);
                list.Add(t);
            }
            return list;
        }


        public static object GetSingle(string strSql)
        {
            return DbHelperSQL.GetSingle(strSql);
        }


        public static DataTable GetTable(string strSql)
        {
            return DbHelperSQL.Query(strSql).Tables[0];
        }

        public static DataSet GetDataSet(string strSql)
        {
            return DbHelperSQL.Query(strSql);
        }

        public static void BackUpDataBase(string dataBaseName, string folder, string backUpName)
        {
            DbHelperSQL.BackUpDataBase(dataBaseName, folder, backUpName);
        }
        public static void RunSql(string strSql)
        {
            DbHelperSQL.ExecuteSql(strSql);
        }
        public static bool RunSqlReturnBool(string strSql)
        {
            try
            {
                DbHelperSQL.ExecuteSql(strSql);
                return true;
            }
            catch
            {
                return false;
            }
        }

        private static string CommDataConn
        {
            get
            {
                return PubConstant.CompanyConnectionString;
            }
        }

        #endregion

        #region 通用泛型方法增删改查

        /// <summary>
        /// 得到一个Model，根据主键查询
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="ID"></param>
        /// <returns></returns>
        public static T GetModel<T>(object ID, string[] fieldsName) where T : new()
        {
            Type type = typeof(T);
            string tableName = GetTableName(type);
            PropertyInfo[] propertyArray = type.GetProperties(); //获取所有的公有属性
            List<PropertyInfo> propList = new List<PropertyInfo>();
            StringBuilder strSql = new StringBuilder();
            strSql.Append("SELECT ");
            //List<string> keys = new List<string>();
            //暂时只支持一个主键
            string keys = string.Empty;
            List<SqlParameter> param = new List<SqlParameter>();
            string tempPropName = string.Empty;
            foreach(PropertyInfo property in propertyArray)
            {
                tempPropName = property.Name;
                object[] objArray = property.GetCustomAttributes(typeof(EnitityMappingAttribute), true);
                if(objArray.Length > 0)
                {
                    EnitityMappingAttribute propAttr = objArray[0] as EnitityMappingAttribute;
                    if(propAttr.ColumnType == "IGNORE")
                        continue;
                    else if(propAttr.ColumnType == "KEY")
                    {
                        keys = tempPropName;
                        param.Add(new SqlParameter("@" + tempPropName, ID));
                    }
                }
                if(fieldsName.Length > 0 && fieldsName.Contains(tempPropName))
                {
                    strSql.Append(tempPropName + ",");
                    propList.Add(property);
                }
            }
            strSql.Remove(strSql.Length - 1, 1);
            strSql.Append("  from  " + tableName + " where ");
            strSql.Append(keys + "=@" + keys);

            //new DbHelperSQL(PubConstant.CompanyConnectionString);
            DataSet ds = DbHelperSQL.Query(strSql.ToString(), param.ToArray());
            //实体转换
            if(ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0)
            {
                T t = new T();
                t = TranEntity<T>(propertyArray, ds.Tables[0].Rows[0]);
                return t;
            }
            return default(T);
        }

        public static int GetSingle<T>(string strWhere) where T : new()
        {
            Type type = typeof(T);
            string tableName = GetTableName(type);
            StringBuilder strSql = new StringBuilder();
            strSql.Append("SELECT COUNT(1) FROM " + tableName);
            if(!string.IsNullOrEmpty(strWhere))
            {
                strSql.Append(" WHERE " + strWhere);
            }
            return Convert.ToInt16(DbHelperSQL.GetSingle(strSql.ToString()));
        }

        ///// <summary>
        /// 得到一个Model，根据主键查询
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="ID"></param>
        /// <returns></returns>
        public static T GetModel<T>(object ID) where T : new()
        {
            try
            {
                Type type = typeof(T);
                string tableName = GetTableName(type);
                //type.getp
                PropertyInfo[] propertyArray = type.GetProperties(); //获取所有的公有属性
                StringBuilder strSql = new StringBuilder();
                strSql.Append("SELECT ");
                //List<string> keys = new List<string>();
                //暂时只支持一个主键
                string keys = string.Empty;
                List<SqlParameter> param = new List<SqlParameter>();
                string tempPropName = string.Empty;
                foreach(PropertyInfo property in propertyArray)
                {
                    tempPropName = property.Name;
                    object[] objArray = property.GetCustomAttributes(typeof(EnitityMappingAttribute), true);
                    if(objArray.Length > 0)
                    {
                        EnitityMappingAttribute propAttr = objArray[0] as EnitityMappingAttribute;
                        if(propAttr.ColumnType == "IGNORE")
                            continue;
                        else if(propAttr.ColumnType == "KEY")
                        {
                            keys = tempPropName;
                            param.Add(new SqlParameter("@" + tempPropName, ID));
                            //break;
                        }
                    }
                    strSql.Append(tempPropName + ",");
                }
                if(string.IsNullOrEmpty(keys)) throw new Exception("未设置主键");
                strSql.Remove(strSql.Length - 1, 1);
                strSql.Append("  from  " + tableName + " where ");
                strSql.Append(keys + "=@" + keys);
                DataSet ds = DbHelperSQL.Query(strSql.ToString(), param.ToArray());
                //实体转换
                if(ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0)
                {
                    T t = new T();
                    t = TranEntity<T>(propertyArray, ds.Tables[0].Rows[0]);
                    return t;
                }
                return default(T);
            }
            catch(Exception ex)
            {
                throw ex;
            }
        }
        private static T TranEntity<T>(PropertyInfo[] propertys, DataRow dr) where T : new()
        {
            string tempName = string.Empty;
            T t = new T();
            //遍历该对象的所有属性
            foreach(PropertyInfo pi in propertys)
            {
                tempName = pi.Name;//将属性名称赋值给临时变量  
                //检查DataTable是否包含此列（列名==对象的属性名）    
                if(dr.Table.Columns.Contains(tempName))
                {
                    // 判断此属性是否有Setter  
                    if(!pi.CanWrite) continue;//该属性不可写，直接跳出  
                    //取值  
                    object value = dr[tempName];
                    //如果非空，则赋给对象的属性  
                    if(value != DBNull.Value)
                        pi.FastSetValue(t, value);
                }
            }
            return t;
        }
        /// <summary>
        /// 更新一个实体,添加到事务列表中
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="model"></param>
        /// <param name="comm"></param>
        /// <returns></returns>
        public static List<CommonObject> Update<T>(T model, List<CommonObject> comm)
        {
            try
            {
                StringBuilder strSql = new StringBuilder();
                Type type = typeof(T);
                string tableName = GetTableName(type);

                strSql.Append("update " + tableName + " set ");
                PropertyInfo[] propertyArray = type.GetProperties(); //获取所有的公有属性
                string keys = string.Empty, tempPropName = string.Empty;
                List<SqlParameter> param = new List<SqlParameter>();

                foreach(PropertyInfo property in propertyArray)
                {
                    //if (string.IsNullOrEmpty(keys))
                    //    keys = GetTableKey(property, tempPropName);
                    ////如果主键是Int或long的自增类型，就不更新该字段
                    //if ((!string.IsNullOrEmpty(keys) && property.PropertyType.FullName == "System.Int32") || (!string.IsNullOrEmpty(keys) && property.PropertyType.FullName == "System.Int64"))
                    //    continue;
                    object[] objArray = property.GetCustomAttributes(typeof(EnitityMappingAttribute), true);
                    if(objArray.Length > 0)
                    {
                        EnitityMappingAttribute propAttr = objArray[0] as EnitityMappingAttribute;
                        if(propAttr.ColumnType == "IGNORE")
                            continue;
                        if(propAttr.ColumnType == "KEY")
                        {
                            keys = property.Name;// GetTableKey(property, tempPropName);
                            param.Add(new SqlParameter("@" + keys, property.FastGetValue(model)));
                            continue;
                        }

                    }
                    tempPropName = property.Name;
                    strSql.Append(tempPropName + " = @" + tempPropName + ",");
                    param.Add(new SqlParameter("@" + tempPropName, property.FastGetValue(model)));
                }
                strSql.Remove(strSql.Length - 1, 1);
                strSql.Append(" where ");
                strSql.Append(keys + " = @" + keys);
                comm.Add(new CommonObject(strSql.ToString(), param.ToArray()));
                return comm;
            }
            catch(Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// 更新实体类中部分字段
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="model"></param>
        /// <param name="fields">需要更新的字段的数组</param>
        /// <param name="comm"></param>
        /// <returns></returns>
        public static List<CommonObject> Update<T>(T model, string[] fields, List<CommonObject> comm)
        {
            try
            {
                StringBuilder strSql = new StringBuilder();
                Type type = typeof(T);
                string tableName = GetTableName(type);

                strSql.Append("update " + tableName + " set ");
                PropertyInfo[] propertyArray = type.GetProperties(); //获取所有的公有属性
                List<PropertyInfo> listProp = new List<PropertyInfo>();
                string keys = string.Empty, tempPropName = string.Empty;
                List<SqlParameter> param = new List<SqlParameter>();
                foreach(PropertyInfo property in propertyArray)
                {
                    tempPropName = property.Name;
                    if(string.IsNullOrEmpty(keys))
                    {
                        keys = GetTableKey(property, tempPropName);
                        param.Add(new SqlParameter("@" + tempPropName, property.FastGetValue(model)));
                    }
                    if(fields.Length > 0 && fields.Contains(tempPropName))
                    {
                        strSql.Append(tempPropName + " = @" + tempPropName + ",");
                        listProp.Add(property);
                        param.Add(new SqlParameter("@" + tempPropName, property.FastGetValue(model)));
                    }
                }
                strSql.Remove(strSql.Length - 1, 1);
                strSql.Append(" where ");
                strSql.Append(keys + " = @" + keys);
                comm.Add(new CommonObject(strSql.ToString(), param.ToArray()));
                return comm;
            }
            catch(Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// 获取主键列
        /// </summary>
        /// <param name="property"></param>
        /// <param name="tempPropName"></param>
        /// <returns></returns>
        private static string GetTableKey(PropertyInfo property, string tempPropName)
        {
            string keys = string.Empty;
            object[] objArray = property.GetCustomAttributes(typeof(EnitityMappingAttribute), true);
            if(objArray.Length > 0)
            {
                EnitityMappingAttribute propAttr = objArray[0] as EnitityMappingAttribute;
                if(propAttr.ColumnType.Equals("KEY"))
                {
                    keys = tempPropName;
                }
            }
            return keys;
        }
        /// <summary>
        /// 获取表明
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        private static string GetTableName(Type type)
        {
            string tableName = string.Empty;
            object[] objArray = type.GetCustomAttributes(typeof(EnitityMappingAttribute), false);
            if(objArray.Length > 0)
            {
                EnitityMappingAttribute attr = objArray[0] as EnitityMappingAttribute;
                tableName = attr.TableName;
            }
            else
            {
                tableName = type.Name;
            }
            return tableName;
        }
        /// <summary>
        /// 更新一个实体，直接更新
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="model"></param>
        /// <returns></returns>
        public static bool Update<T>(T model)
        {
            return CommonBase.RunListCommit(Update(model, new List<CommonObject>()));
        }
        /// <summary>
        /// 更新部分字段，
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="model"></param>
        /// <param name="fieldName">需要更新的字段数组</param>
        /// <returns></returns>
        public static bool Update<T>(T model, string[] fieldName)
        {
            return CommonBase.RunListCommit(Update(model, fieldName, new List<CommonObject>()));
        }
        /// <summary>
        /// 插入一个实体，添加到执行事务列表中
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="model"></param>
        /// <param name="comm"></param>
        /// <returns></returns>
        public static List<CommonObject> Insert<T>(T model, List<CommonObject> comm)
        {
            try
            {
                StringBuilder strSql = new StringBuilder();
                Type type = typeof(T);
                string tableName = GetTableName(type);
                strSql.Append("insert into " + tableName + "( ");
                string insertFields = string.Empty, insertFieldParams = string.Empty, tempPropName = string.Empty;
                List<SqlParameter> param = new List<SqlParameter>();
                PropertyInfo[] propertyArray = type.GetProperties(); //获取所有的公有属性
                string keys = string.Empty;
                foreach(PropertyInfo property in propertyArray)
                {
                    tempPropName = property.Name;
                    //如果主键是Int或long的自增类型，就不插入
                    if(string.IsNullOrEmpty(keys))
                    {
                        keys = GetTableKey(property, tempPropName);
                        if((!string.IsNullOrEmpty(keys) && property.PropertyType.FullName == "System.Int32") || (!string.IsNullOrEmpty(keys) && property.PropertyType.FullName == "System.Int64"))
                        {
                            continue;
                        }
                    }
                    object[] objArray = property.GetCustomAttributes(typeof(EnitityMappingAttribute), true);
                    if(objArray.Length > 0)
                    {
                        EnitityMappingAttribute propAttr = objArray[0] as EnitityMappingAttribute;
                        if(propAttr.ColumnType == "IGNORE")
                            continue;
                    }

                    insertFields += tempPropName + ",";
                    insertFieldParams += "@" + tempPropName + ",";
                    param.Add(new SqlParameter("@" + tempPropName, property.FastGetValue(model)));
                }
                insertFields = insertFields.Remove(insertFields.Length - 1, 1);
                insertFieldParams = insertFieldParams.Remove(insertFieldParams.Length - 1, 1);
                strSql.Append(insertFields + ") values (" + insertFieldParams + ") ;");
                comm.Add(new CommonObject(strSql.ToString(), param.ToArray()));
                return comm;
            }
            catch(Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// 插入一个实体，直接插入
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="model"></param>
        /// <returns></returns>
        public static bool Insert<T>(T model)
        {
            return CommonBase.RunListCommit(Insert(model, new List<CommonObject>()));
        }
        /// <summary>
        /// 根据实体一条数据，添加到删除事务执行列表中
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="model"></param>
        /// <param name="comm"></param>
        /// <returns></returns>
        public static List<CommonObject> Delete<T>(T model, List<CommonObject> comm)
        {
            try
            {
                StringBuilder strSql = new StringBuilder();
                Type type = typeof(T);
                string tableName = GetTableName(type);
                strSql.Append("delete from  " + tableName);
                object keyValues = null;
                //获得主键
                string keys = string.Empty, tempPropName = string.Empty;
                foreach(PropertyInfo property in type.GetProperties())
                {
                    tempPropName = property.Name;
                    if(string.IsNullOrEmpty(keys))
                    {
                        keys = GetTableKey(property, tempPropName);
                    }
                    if(!string.IsNullOrEmpty(keys))
                    {
                        keyValues = property.FastGetValue(model);
                        break;
                    }
                }
                if(string.IsNullOrEmpty(keys)) throw new Exception("未设置主键");
                if(keyValues == null || string.IsNullOrEmpty(keyValues.ToString())) throw new Exception("主键值不能为空");
                strSql.AppendFormat(" where " + keys + "='{0}'", keyValues);
                comm.Add(new CommonObject(strSql.ToString(), null));
                return comm;
            }
            catch(Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// 删除一个实体，直接删除
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="model"></param>
        /// <returns></returns>
        public static bool Delete<T>(T model)
        {
            return CommonBase.RunListCommit(Delete(model, new List<CommonObject>()));
        }

        /// <summary>
        /// 获得实体列表
        /// </summary>
        public static List<T> GetList<T>(string strWhere = "", int top = 0, string orderBy = "") where T : new()
        {
            List<T> list = new List<T>();
            StringBuilder strSql = new StringBuilder();
            Type type = typeof(T);
            string tableName = GetTableName(type);
            strSql.Append("select  *  from  " + tableName);
            if(top > 0)
            {
                strSql.Clear();
                strSql.Append("select top " + top + "  *  from  " + tableName);
            }
            if(!string.IsNullOrEmpty(strWhere))
            {
                strSql.Append(" where " + strWhere);
            }
            if(!string.IsNullOrEmpty(orderBy))
            {
                strSql.Append(" order by  " + orderBy);
            }
            DataTable table = GetTable(strSql.ToString());
            foreach(DataRow de in table.Rows)
            {
                T t = new T();
                PropertyInfo[] propertys = t.GetType().GetProperties();
                t = TranEntity<T>(propertys, de);
                list.Add(t);
            }
            return list;
        }
        /// <summary>
        /// 获取列表中的部分字段，不分页
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fields"></param>
        /// <param name="strWhere"></param>
        /// <param name="top"></param>
        /// <param name="orderBy"></param>
        /// <returns></returns>
        public static List<T> GetList<T>(string[] fields, string strWhere = "", int top = 0, string orderBy = "") where T : new()
        {
            List<T> list = new List<T>();
            StringBuilder strSql = new StringBuilder();
            Type type = typeof(T);
            string tableName = GetTableName(type);
            strSql.Append("select ");
            if(top > 0)
            {
                strSql.Append(" top " + top);
            }
            if(fields.Length > 0)
            {
                string tempPropName = string.Empty;
                foreach(string str in fields)
                {
                    strSql.Append(str + ",");
                }
                strSql.Remove(strSql.Length - 1, 1);
                strSql.Append("  from  " + tableName);
            }
            else
            {
                strSql.Append(" * from  " + tableName);
            }

            if(!string.IsNullOrEmpty(strWhere))
            {
                strSql.Append(" where " + strWhere);
            }
            if(!string.IsNullOrEmpty(orderBy))
            {
                strSql.Append(" order by  " + orderBy);
            }
            DataTable table = GetTable(strSql.ToString());
            foreach(DataRow de in table.Rows)
            {
                T t = new T();
                t = TranEntity<T>(t.GetType().GetProperties(), de);
                list.Add(t);
            }
            return list;
        }

        /// <summary>
        /// 获得分页实体列表,使用存储过程
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="strWhere"></param>
        /// <param name="top"></param>
        /// <param name="orderBy"></param>
        /// <returns></returns>
        public static List<T> GetPageList<T>(string strWhere, int pageIndex, int pageSize, out int count) where T : new()
        {
            List<T> list = new List<T>();
            StringBuilder strSql = new StringBuilder();
            Type type = typeof(T);
            string tableName = GetTableName(type);
            //获得主键
            PropertyInfo[] propertys = type.GetProperties();
            string keys = string.Empty, tempPropName = string.Empty;
            foreach(PropertyInfo property in propertys)
            {
                tempPropName = property.Name;
                if(string.IsNullOrEmpty(keys))
                {
                    keys = GetTableKey(property, tempPropName);
                }
                if(!string.IsNullOrEmpty(keys))
                    break;
            }
            if(string.IsNullOrEmpty(keys)) throw new Exception("未设置主键");
            DataTable table = CommonBase.GetTable(tableName + " TMP ", keys, keys + " DESC", strWhere, pageIndex, pageSize, out count);
            foreach(DataRow de in table.Rows)
            {
                T t = new T();
                t = TranEntity<T>(propertys, de);
                list.Add(t);
            }
            return list;
        }

        public static List<T> GetPageList<T>(string joinTableName, string strWhere, int pageIndex, int pageSize, out int count) where T : new()
        {
            List<T> list = new List<T>();
            StringBuilder strSql = new StringBuilder();
            Type type = typeof(T);
            string tableName = GetTableName(type);
            //获得主键
            PropertyInfo[] propertys = type.GetProperties();
            string keys = string.Empty, tempPropName = string.Empty;
            foreach(PropertyInfo property in propertys)
            {
                tempPropName = property.Name;
                if(string.IsNullOrEmpty(keys))
                {
                    keys = GetTableKey(property, tempPropName);
                }
                if(!string.IsNullOrEmpty(keys))
                    break;
            }
            if(string.IsNullOrEmpty(keys)) throw new Exception("未设置主键");
            DataTable table = CommonBase.GetTable(tableName + " TMP " + joinTableName, keys, keys + " DESC", strWhere, pageIndex, pageSize, out count);
            foreach(DataRow de in table.Rows)
            {
                T t = new T();
                t = TranEntity<T>(propertys, de);
                list.Add(t);
            }
            return list;
        }

        #endregion


        #region 从XML配置中读取复杂的SQL语句

        private static string GetSqlFromXml(string sqlKey)
        {
            string sql = string.Empty;
            string configPathName = "Config\\SqlDao.xml";
            string path = AppDomain.CurrentDomain.BaseDirectory + configPathName;
            if(!string.IsNullOrEmpty(path))
            {
                XmlDocument xmlDoc = new XmlDocument();
                try
                {
                    xmlDoc.Load(path);
                    XmlNode nodeList = xmlDoc.GetElementsByTagName("property").Item(0);
                    foreach(XmlNode nd in nodeList.ChildNodes)
                    {
                        if(nd.Attributes["key"].Value == sqlKey)
                        {
                            sql = nd["value"].InnerText;
                            break;
                        }
                    }
                    return sql;
                }
                catch
                {
                    throw new Exception("未找到xml配置文件，或xml文件配置错误");
                }
            }
            else
            {
                return "未找到xml配置文件";
            }

        }
        /// <summary>
        /// 从xml中读取数据,复杂的SQL语句可以使用这种方式执行
        /// </summary>
        /// <param name="xmlKey">SqlDao中的key标志</param>
        /// <param name="hsParam">参数Hashtable</param>
        /// <returns>返回DataSet</returns>
        public static DataSet GetDataSet(string xmlKey, Hashtable hsParam)
        {
            string strSql = GetSqlFromXml(xmlKey);
            List<SqlParameter> param = new List<SqlParameter>();
            foreach(DictionaryEntry de in hsParam)
            {
                param.Add(new SqlParameter(de.Key.ToString(), de.Value));
            }
            return DbHelperSQL.Query(strSql, param.ToArray());
        }
        #endregion
    }
}
