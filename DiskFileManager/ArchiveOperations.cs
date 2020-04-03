using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DiskFileManager {
	public static class ArchiveOperations {
		public static List<Archive> GetKnownArchives(SQLiteConnection connection) {
			List<Archive> archives = new List<Archive>();
			using (IDbTransaction t = connection.BeginTransaction()) {
				List<object[]> rv = HyoutaTools.SqliteUtil.SelectArray(t, "SELECT id FROM Archives ORDER BY id ASC", new object[0]);
				if (rv != null) {
					foreach (object[] r in rv) {
						archives.Add(new Archive() { ArchiveId = (long)r[0], Paths = new List<ArchivePath>(), Patterns = new List<ArchivePattern>() });
					}
				}

				foreach (Archive a in archives) {
					List<object[]> rv2 = HyoutaTools.SqliteUtil.SelectArray(t,
						"SELECT ArchivePaths.pathId AS pathId, Paths.volumeId AS volumeId, Pathnames.name AS pathname " +
						"FROM ArchivePaths " +
						"INNER JOIN Paths ON ArchivePaths.pathId = Paths.id " +
						"INNER JOIN Pathnames ON Paths.pathnameId = Pathnames.id " +
						"WHERE ArchivePaths.archiveId = ?", new object[] { a.ArchiveId }
					);
					if (rv2 != null) {
						foreach (object[] r in rv2) {
							a.Paths.Add(new ArchivePath() {
								PathId = (long)r[0],
								VolumeId = (long)r[1],
								Path = (string)r[2],
							});
						}
					}

					List<object[]> rv3 = HyoutaTools.SqliteUtil.SelectArray(t,
						"SELECT id, pattern, timestampBegin, timestampEnd " +
						"FROM ArchivePatterns " +
						"WHERE ArchivePatterns.archiveId = ?", new object[] { a.ArchiveId }
					);
					if (rv3 != null) {
						foreach (object[] r in rv3) {
							a.Patterns.Add(new ArchivePattern() {
								ArchivePatternId = (long)r[0],
								Pattern = (string)r[1],
								TimestampBegin = HyoutaTools.Util.UnixTimeToDateTime((long)r[2]),
								TimestampEnd = HyoutaTools.Util.UnixTimeToDateTime((long)r[3]),
							});
						}
					}
				}
			}
			return archives;
		}

		public static int AddPathToArchive(TextWriter writer, SQLiteConnection connection, long archiveId, string path) {
			List<Volume> volumes = VolumeOperations.FindAndInsertAttachedVolumes(connection);
			string devId = Win32Util.FindVolumeIdFromPath(path);
			Volume vol = volumes.First(x => x.DeviceID == devId);

			// messy nonsense
			var fi = new FileInfo(path);
			string subpath = fi.FullName.Substring(Path.GetPathRoot(fi.FullName).Length).Replace("\\", "/");
			if (!subpath.StartsWith("/")) {
				subpath = "/" + subpath;
			}
			if (subpath.EndsWith("/")) {
				subpath = subpath.Substring(0, subpath.Length - 1);
			}
			if (subpath == "" || subpath == "/") {
				return -1;
			}

			using (IDbTransaction t = connection.BeginTransaction()) {
				long pathId = DatabaseHelper.InsertOrUpdatePath(t, vol.ID, subpath);
				HyoutaTools.SqliteUtil.Update(t, "INSERT INTO ArchivePaths (archiveId, pathId) VALUES (?, ?)", new object[] { archiveId, pathId });
				t.Commit();
			}

			return 0;
		}

		public static int AddPatternToArchive(TextWriter writer, SQLiteConnection connection, long archiveId, string pattern, DateTime begin, DateTime end) {
			using (IDbTransaction t = connection.BeginTransaction()) {
				long b = HyoutaTools.Util.DateTimeToUnixTime(begin);
				long e = HyoutaTools.Util.DateTimeToUnixTime(end) - 1;
				HyoutaTools.SqliteUtil.Update(t, "INSERT INTO ArchivePatterns (archiveId, pattern, timestampBegin, timestampEnd) VALUES (?, ?, ?, ?)", new object[] { archiveId, pattern, b, e });
				t.Commit();
			}

			return 0;
		}

		public static int PrintExistingArchives(string logPath, string databasePath) {
			using (TextWriterWrapper textWriterWrapper = new TextWriterWrapper(logPath))
			using (SQLiteConnection connection = new SQLiteConnection("Data Source=" + databasePath)) {
				connection.Open();
				List<Volume> volumes = VolumeOperations.GetKnownVolumes(connection);
				List<Archive> archives = GetKnownArchives(connection);
				connection.Close();

				foreach (Archive a in archives) {
					textWriterWrapper.Writer.WriteLine("Archive #{0}:", a.ArchiveId);
					foreach (ArchivePath p in a.Paths) {
						textWriterWrapper.Writer.WriteLine("  Volume #{0} {1}{2}", p.VolumeId, volumes.First(x => x.ID == p.VolumeId).Label, p.Path);
					}
					foreach (ArchivePattern p in a.Patterns) {
						textWriterWrapper.Writer.WriteLine("  Pattern #{0}: {1} from {2} to {3}", p.ArchivePatternId, p.Pattern, p.TimestampBegin, p.TimestampEnd);
					}
				}
			}
			return 0;
		}
	}
}
