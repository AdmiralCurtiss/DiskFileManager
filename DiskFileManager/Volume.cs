using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DiskFileManager {
	public class Volume {
		public long ID;
		public string DeviceID;
		public string Label;
		public long TotalSpace;
		public long FreeSpace;
		public bool ShouldScan;
	}
}
