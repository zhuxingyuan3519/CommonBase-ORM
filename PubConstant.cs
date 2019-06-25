using System;
using System.Configuration;
namespace DBUtility
{

    public class PubConstant
    {
        /// <summary>
        /// ��ȡ�����ַ���
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
        /// �õ�web.config������������ݿ������ַ�����
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
