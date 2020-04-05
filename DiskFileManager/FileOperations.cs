using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DiskFileManager {
	public static class FileOperations {
		public static (long fileId, DateTime timestamp) InsertOrGetFile(IDbTransaction t, long filesize, byte[] hash, byte[] shorthash, DateTime lastWriteTimeUtc) {
			var rv = HyoutaTools.SqliteUtil.SelectArray(t, "SELECT id, timestamp FROM Files WHERE size = ? AND hash = ? AND shorthash = ?", new object[] { filesize, hash, shorthash });
			if (rv == null || rv.Count == 0) {
				long timestamp = HyoutaTools.Util.DateTimeToUnixTime(lastWriteTimeUtc);
				HyoutaTools.SqliteUtil.Update(t, "INSERT INTO Files ( size, hash, shorthash, timestamp ) VALUES ( ?, ?, ?, ? )", new object[] { filesize, hash, shorthash, timestamp });
				rv = HyoutaTools.SqliteUtil.SelectArray(t, "SELECT id, timestamp FROM Files WHERE size = ? AND hash = ? AND shorthash = ?", new object[] { filesize, hash, shorthash });
			}
			return ((long)rv[0][0], HyoutaTools.Util.UnixTimeToDateTime((long)rv[0][1]));
		}

		public static FileIdentity IdentifyFile(string path) {
			long filesize;
			byte[] shorthash;
			byte[] hash;
			using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read)) {
				filesize = fs.Length;
				shorthash = HashUtil.CalculateShortHash(fs);
				hash = HashUtil.CalculateHash(fs);
			}
			return new FileIdentity(filesize, shorthash, hash);
		}
	}
}
