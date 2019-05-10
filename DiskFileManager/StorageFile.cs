using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DiskFileManager {
	class StorageFile {
		public long FileId;
		public long Size;
		public byte[] ShortHash;
		public byte[] Hash;
		public long StorageId;
		public string Path;
		public string Filename;
		public long VolumeId;
		public DateTime Timestamp;
		public DateTime LastSeen;
	}
}
