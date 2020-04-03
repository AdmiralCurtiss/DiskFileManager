using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DiskFileManager {
	public class Archive {
		public long ArchiveId;
		public List<ArchivePath> Paths;
		public List<ArchivePattern> Patterns;
	}

	public class ArchivePath {
		public long PathId;
		public long VolumeId;
		public string Path;
	}

	public class ArchivePattern {
		public long ArchivePatternId;
		public string Pattern;
		public DateTime TimestampBegin;
		public DateTime TimestampEnd;
	}
}
