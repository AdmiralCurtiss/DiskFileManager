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

		public static void DoArchiveScan(TextWriter writer, SQLiteConnection connection, string scanPath) {
			bool fileExists = new FileInfo(scanPath).Exists;
			bool dirExists = new DirectoryInfo(scanPath).Exists;
			if (fileExists == dirExists) {
				return;
			}

			List<Volume> volumes = VolumeOperations.FindAndInsertAttachedVolumes(connection);
			List<Archive> archives = GetKnownArchives(connection);
			if (fileExists) {
				ScanFile(writer, connection, volumes, archives, new FileInfo(scanPath));
			} else {
				ScanDirectory(writer, connection, volumes, archives, scanPath);
			}
		}

		public static void ScanDirectory(TextWriter writer, SQLiteConnection connection, List<Volume> volumes, List<Archive> archives, string path) {
			writer.WriteLine("Scanning {0}...", path);
			foreach (var fsi in new DirectoryInfo(path).GetFileSystemInfos()) {
				if (fsi is FileInfo) {
					ScanFile(writer, connection, volumes, archives, fsi as FileInfo);
				} else if (fsi is DirectoryInfo) {
					ScanDirectory(writer, connection, volumes, archives, fsi.FullName);
				}
			}
		}

		public static void ScanFile(TextWriter writer, SQLiteConnection connection, List<Volume> volumes, List<Archive> archives, FileInfo file) {
			// identify this file
			writer.WriteLine();
			writer.WriteLine("Identifying {0}...", file.FullName);
			FileIdentity identity = FileOperations.IdentifyFile(file.FullName);
			DateTime timestamp;
			using (IDbTransaction t = connection.BeginTransaction()) {
				timestamp = FileOperations.InsertOrGetFile(t, identity.Filesize, identity.Hash, identity.ShortHash, file.LastWriteTimeUtc).timestamp;
				t.Commit();
			}

			// with this canonical timestamp, find all archive patterns that match this file
			string name = file.Name;
			List<Archive> matchingArchives = new List<Archive>();
			foreach (Archive a in archives) {
				foreach (ArchivePattern p in a.Patterns) {
					if (timestamp >= p.TimestampBegin && timestamp <= p.TimestampEnd && MatchPattern(name, p.Pattern)) {
						matchingArchives.Add(a);
						break;
					}
				}
			}

			if (matchingArchives.Count == 0) {
				writer.WriteLine("File {0} does not fit into any archive.", file.FullName);
				return;
			}

			if (matchingArchives.Count > 1) {
				writer.WriteLine("File {0} does fit into {1} archives. Archive configuration error?", file.FullName, matchingArchives.Count);
				return;
			}

			if (matchingArchives.Count != 1) {
				throw new Exception("???");
			}

			Archive matchingArchive = matchingArchives[0];
			if (matchingArchive.Paths.Count <= 0) {
				writer.WriteLine("File {0} fits into archive, but archive has no defined paths.", file.FullName);
				return;
			}

			writer.WriteLine("File {0} fits into archive {1}", file.FullName, matchingArchive.ArchiveId);

			// alright now we have to
			// - copy this file into every path
			// - if and only if the file was successfully copied into every path, delete it from the source, otherwise leave it alone
			// be sure to special case things like current file == target file!!
			long successfulCopies = 0;
			bool allowDelete = true;
			string sourcePath = file.FullName;
			foreach (ArchivePath p in matchingArchive.Paths) {
				try {
					Volume vol = volumes.FirstOrDefault(x => x.ID == p.VolumeId);
					if (vol == null) {
						writer.WriteLine("Volume {0} appears to not be attached.", p.VolumeId);
						allowDelete = false;
						continue;
					}

					string targetPath = CreateFilePath(vol.DeviceID, p.Path, name);
					bool existedBefore = File.Exists(targetPath);
					if (!existedBefore) {
						// file does not exist at target yet, copy over
						writer.WriteLine("Copying file to {0}...", targetPath);
						File.Copy(sourcePath, targetPath);
					}

					if (File.Exists(targetPath)) {
						writer.WriteLine("Identifying {0}...", targetPath);
						FileIdentity existingTargetIdentity = FileOperations.IdentifyFile(targetPath);
						if (identity == existingTargetIdentity) {
							if (existedBefore) {
								// TODO: check here if source file == target file!!!
								// for now to be safe just inhibit delete
								allowDelete = false;
							}
							writer.WriteLine("File at {0} exists and matches.", targetPath);
							++successfulCopies;
							continue;
						} else {
							writer.WriteLine("File at {0} exists and DOES NOT match!", targetPath);
							allowDelete = false;
							continue;
						}
					} else {
						writer.WriteLine("File at {0} somehow disappeared???", targetPath);
						allowDelete = false;
						continue;
					}
				} catch (Exception ex) {
					writer.WriteLine("Failed to process file to path {0}/{1}: {2}", p.VolumeId, p.Path, ex.ToString());
					allowDelete = false;
				}
			}

			if (successfulCopies == matchingArchive.Paths.Count) {
				if (allowDelete) {
					writer.WriteLine("Deleting {0}", sourcePath);
					File.Delete(sourcePath);
				} else {
					writer.WriteLine("Inhibited delete of {0} despite all targets existing.", sourcePath);
				}
			} else {
				writer.WriteLine("Could not copy or confirm all targets of {0}", sourcePath);
			}

			return;
		}

		private static string CreateFilePath(string deviceId, string directory, string file) {
			string dir = directory.StartsWith("/") ? directory.Substring(1) : directory;
			if (dir == "") {
				return Path.Combine(deviceId, file);
			}
			return Path.Combine(deviceId, dir, file);
		}

		private static bool MatchPattern(string name, string pattern) {
			// this is really hacky but it seems like this isn't anywhere in the C# default library and I don't feel like actually implementing this...
			if (pattern.Count(x => x == '*') == 1) {
				var ps = pattern.Split('*');
				if (name.Length >= ps[0].Length + ps[1].Length) {
					return name.StartsWith(ps[0]) && name.EndsWith(ps[1]);
				}
				return false;
			}
			throw new Exception("pattern matching for " + pattern + " not implemented");
		}
	}
}
