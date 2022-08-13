using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Capsulemon.Scheduler.Models
{
    public abstract class ServerDesc
    {
        public string UUID { get; set; }
        public string HostName { get; set; }
        public string IP { get; set; }
        public DateTime dtmStarted { get; set; }
        //[JsonConverter(typeof(IsoDateTimeConverter))]
        public DateTime dtmUpdated { get; set; }
        public string Version { get; set; }
        public string Revision { get; set; }
        public string ResourceRev { get; set; }
        [Display(Name = "Matchmaker")]
        public string MatchmakerVersion { get; set; } = string.Empty;
        public string MatchmakerRevision { get; set; } = string.Empty;
        public string MatchmakerResourceRevision { get; set; } = string.Empty;
        public string MatchmakerIP { get; set; } = string.Empty;
        public int MatchmakerPort { get; set; } = 0;
        public string MatchmakerAddr { get { return string.IsNullOrEmpty(MatchmakerIP) ? string.Empty : MatchmakerIP + ":" + MatchmakerPort; } }
    }
    public class GameServerDesc : ServerDesc
    {
        public string PublicIP { get; set; }
        public int numPort { get; set; }
        public string PublicUrl { get; set; }
        [Display(Name = "MaxUser")]
        public int numMaxUser { get; set; }
        [Display(Name = "CCU")]
        public int numCCU { get; set; }
        public bool IsActive { get; set; } = true;
    }
}
