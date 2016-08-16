using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AWHReports.Infrastructure.Configuration
{

    public static class ConnectionStringFactory
    {
        public static string JCDSConnectionString { get { return ConfigurationManager.ConnectionStrings["ConnJCJT"].ToString(); } }
    }
}
