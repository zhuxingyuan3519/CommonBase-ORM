using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBUtility
{
    public class CommonObject
    {
        public string CommandText { set; get; }
        public SqlParameter[] parameters { set; get; }
        public CommonObject(string comText, SqlParameter[] sqlPara)
        {
            this.CommandText = comText;
            this.parameters = sqlPara;
        }
    }

    public class EnitityMappingAttribute : Attribute
    {
        private string tableName;
        /// <summary>  
        /// 实体实际对应的表名  
        /// </summary>  
        public string TableName
        {
            get { return tableName; }
            set { tableName = value; }
        }

        private string columnType;
        /// <summary>  
        /// 列的类型，KEY=主键，删除、更新、查询会根据这个主键查询，暂时只支持一个主键，就是所有的字段只有而且仅有一个 [EnitityMapping(ColumnType = "KEY")]属性，
        /// IGNORE=忽视的列，增删改查会忽视该列，当自定义一些属性并且数据库中没有这一列，就可以使用该属性IGNORE
        /// </summary>  
        public string ColumnType
        {
            get { return columnType; }
            set { columnType = value; }
        }
    }
}
