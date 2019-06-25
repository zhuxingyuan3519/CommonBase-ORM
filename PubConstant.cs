using System;
using System.Configuration;
namespace DBUtility
{

    public class PubConstant
    {
        /// <summary>
        /// 获取连接字符串
        /// </summary>
        public static string ConnectionString
        {
            get
            {
                string _connectionString = GetConnectionString("zxConn");
                return _connectionString;
            }
        }

        public static string CompanyConnectionString
        {
            get
            {
                string _connectionString = GetConnectionString("commonConn");
                return _connectionString;
            }
        }

        /// <summary>
        /// 得到web.config里配置项的数据库连接字符串。
        /// </summary>
        /// <param name="configName"></param>
        /// <returns></returns>
        public static string GetConnectionString(string configName)
        {
            try
            {
                string connectionString = ConfigurationManager.ConnectionStrings[configName].ConnectionString;
                string ConStringEncrypt = ConfigurationManager.AppSettings["ConStringEncrypt"];
                if (ConStringEncrypt == "true")
                {
                    connectionString = DESEncrypt.Decrypt(connectionString);
                }
                return connectionString;
            }
            catch
            {
                return string.Empty;
            }
        }


    }
}
