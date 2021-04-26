using CommandLine;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Linq;

namespace DiskFileManager {
	public class BaseOptions {
		[Option("database", Default = null, Required = false, HelpText = "Custom path to database.")]
		public string DatabasePath {
			get {
				return _DatabasePath ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "diskfilemanagerdb.sqlite");
			}
			set {
				_DatabasePath = value;
			}
		}

		private string _DatabasePath;

		[Option("log", Default = null, Required = false, HelpText = "Path to log file. Prints to stdout if not set.")]
		public string LogPath { get; set; }
	}

	[Verb("scan", HelpText = "Scan currently attached volumes for new, removed, or changed files.")]
	public class ScanOptions : BaseOptions {
		[Option('v', "volume", Default = null, Required = false, HelpText = "Limit scan to given volume ID.")]
		public int? Volume { get; set; }
	}

	[Verb("list", HelpText = "List volumes or files.")]
	public class ListOptions : BaseOptions {
		[Option('v', "volume", Default = null, Required = false, HelpText = "Volume ID to list files of. 0 to list all volumes.")]
		public int? Volume { get; set; }

		[Option("show-disabled", Default = false, Required = false, HelpText = "Also show disabled volumes.")]
		public bool ShowDisabledVolumes { get; set; }

		[Option("selected-volume-only", Default = false, Required = false, HelpText = "Only list files on the given volume.")]
		public bool SelectedVolumeOnly { get; set; }

		[Option("min-instance-count", Default = null, Required = false, HelpText = "Minimum file instance count.")]
		public int? MinInstanceCount { get; set; }

		[Option("max-instance-count", Default = null, Required = false, HelpText = "Maximum file instance count.")]
		public int? MaxInstanceCount { get; set; }
	}

	[Verb("search", HelpText = "Search for files.")]
	public class SearchOptions : BaseOptions {
		[Option('f', "filename", Default = null, Required = true, HelpText = "Search pattern for filename.")]
		public string File { get; set; }
	}

	[Verb("multi", HelpText = "Find files that exist in multiple places on the same volume.")]
	public class QuickfindMultipleOptions : BaseOptions {
		[Option('v', "volume", Required = true, HelpText = "Volume ID to find files of.")]
		public int Volume { get; set; }

		[Option('d', "interactive-delete-mode", Default = false, Required = false, HelpText = "Start interactive duplicate deletion mode.")]
		public bool InteractiveDeleteMode { get; set; }

		[Option("subdir", Default = null, Required = false, HelpText = "Only look in a specific subdirectory.")]
		public string Subdir { get; set; }
	}

	[Verb("exclusive", HelpText = "Find files that only exist on one volume (no backups!).")]
	public class QuickfindExclusiveOptions : BaseOptions {
		[Option('v', "volume", Required = true, HelpText = "Volume ID to find files of.")]
		public int Volume { get; set; }
	}

	[Verb("archive", HelpText = "Define desired paths for files and move them there.")]
	public class ArchiveOptions : BaseOptions {
		[Option('n', "new-archive", Required = false, Default = null, HelpText = "Create a new archive.")]
		public bool? NewArchive { get; set; }

		[Option('m', "modify-archive", Required = false, HelpText = "Modify an existing archive, identified by ID.")]
		public int? ModifyArchiveId { get; set; }

		[Option("add-path", Required = false, HelpText = "Add new path.")]
		public string AddPath { get; set; }

		[Option("add-pattern", Required = false, HelpText = "Add new pattern.")]
		public string AddPattern { get; set; }

		[Option("begin", Required = false, Default = "20000101", HelpText = "Start timestamp for pattern, in format YYYYMMDD.")]
		public string TimestampBegin { get; set; }

		[Option("end", Required = false, Default = "20000101", HelpText = "End timestamp for pattern, in format YYYYMMDD.")]
		public string TimestampEnd { get; set; }

		[Option("scan", Required = false, HelpText = "File or directory to try and archive.")]
		public string ScanPath { get; set; }
	}

	class TextWriterWrapper : IDisposable {
		public TextWriter Writer { get; private set; }

		public TextWriterWrapper(string outpath) {
			Writer = outpath != null ? new StreamWriter(outpath) : Console.Out;
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing && Writer != Console.Out) {
				Writer.Dispose();
			}
		}

		public void Dispose() {
			Dispose(true);
		}
	}

	class Program {
		static int Main(string[] args) {
			return Parser.Default.ParseArguments<ScanOptions, ListOptions, SearchOptions, QuickfindMultipleOptions, QuickfindExclusiveOptions, ArchiveOptions>(args).MapResult(
				(ScanOptions a) => Scan(a),
				(ListOptions a) => List(a),
				(SearchOptions a) => Search(a),
				(QuickfindMultipleOptions a) => QuickfindMultipleCopiesOnSameVolume(a),
				(QuickfindExclusiveOptions a) => QuickfindFilesExclusiveToVolume(a),
				(ArchiveOptions a) => Archive(a),
				errs => -1
			);
		}

		private static int QuickfindMultipleCopiesOnSameVolume(QuickfindMultipleOptions a) {
			ShouldPrint shouldPrint = (x) => x.Count >= 2 && (a.Subdir == null || x.Any((y) => y.Path.StartsWith(a.Subdir, true, System.Globalization.CultureInfo.InvariantCulture)));
			if (a.InteractiveDeleteMode) {
				return RunInteractiveFileDeleteMode(a.DatabasePath, a.Volume, shouldPrint);
			} else {
				return ListFiles(a.LogPath, a.DatabasePath, a.Volume, true, shouldPrint);
			}
		}

		private static int QuickfindFilesExclusiveToVolume(QuickfindExclusiveOptions a) {
			return ListFiles(a.LogPath, a.DatabasePath, a.Volume, false, (x) => x.All((s) => s.VolumeId == x[0].VolumeId));
		}

		private static int Scan(ScanOptions args) {
			using (TextWriterWrapper textWriterWrapper = new TextWriterWrapper(args.LogPath))
			using (SQLiteConnection connection = new SQLiteConnection("Data Source=" + args.DatabasePath)) {
				connection.Open();
				using (IDbTransaction t = connection.BeginTransaction()) {
					DatabaseHelper.CreateTables(t);
					t.Commit();
				}

				var volumes = VolumeOperations.FindAndInsertAttachedVolumes(connection);
				foreach (Volume v in volumes) {
					if (!args.Volume.HasValue || (args.Volume.Value == v.ID)) {
						ProcessVolume(textWriterWrapper.Writer, connection, v);
					}
				}

				connection.Close();
			}

			return 0;
		}

		private static DateTime DateTimeFromYYYYMMDD(string s) {
			return DateTime.ParseExact(s, "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeUniversal);
		}

		private static int Archive(ArchiveOptions a) {
			if (a.NewArchive.HasValue && a.NewArchive.Value) {
				return ProcessNewArchive(a.LogPath, a.DatabasePath);
			}

			if (a.ModifyArchiveId.HasValue) {
				return ProcessModifyArchive(a.LogPath, a.DatabasePath, a.ModifyArchiveId.Value, a.AddPath, a.AddPattern, DateTimeFromYYYYMMDD(a.TimestampBegin), DateTimeFromYYYYMMDD(a.TimestampEnd));
			}

			if (a.ScanPath != null) {
				return ProcessArchiveScan(a.LogPath, a.DatabasePath, a.ScanPath);
			}

			return ArchiveOperations.PrintExistingArchives(a.LogPath, a.DatabasePath);
		}

		private static int ProcessNewArchive(string logPath, string databasePath) {
			int rv = -1;
			using (TextWriterWrapper textWriterWrapper = new TextWriterWrapper(logPath))
			using (SQLiteConnection connection = new SQLiteConnection("Data Source=" + databasePath)) {
				connection.Open();
				using (IDbTransaction t = connection.BeginTransaction()) {
					DatabaseHelper.CreateTables(t);
					rv = HyoutaTools.SqliteUtil.Update(t, "INSERT INTO Archives DEFAULT VALUES") == 1 ? 0 : -1;
					if (rv == 0) {
						t.Commit();
					}
				}
				connection.Close();
			}
			return rv;
		}

		private static int ProcessModifyArchive(string logPath, string databasePath, int archiveId, string addPath, string addPattern, DateTime begin, DateTime end) {
			using (TextWriterWrapper textWriterWrapper = new TextWriterWrapper(logPath))
			using (SQLiteConnection connection = new SQLiteConnection("Data Source=" + databasePath)) {
				connection.Open();
				if (addPath != null) {
					ArchiveOperations.AddPathToArchive(textWriterWrapper.Writer, connection, archiveId, addPath);
				}
				if (addPattern != null) {
					ArchiveOperations.AddPatternToArchive(textWriterWrapper.Writer, connection, archiveId, addPattern, begin, end);
				}
				connection.Close();
			}
			return 0;
		}

		private static int ProcessArchiveScan(string logPath, string databasePath, string scanPath) {
			using (TextWriterWrapper textWriterWrapper = new TextWriterWrapper(logPath))
			using (SQLiteConnection connection = new SQLiteConnection("Data Source=" + databasePath)) {
				connection.Open();
				ArchiveOperations.DoArchiveScan(textWriterWrapper.Writer, connection, scanPath);
				connection.Close();
			}
			return 0;
		}

		private static int List(ListOptions args) {
			if (args.Volume.HasValue) {
				int? volume = null;
				if (args.Volume.Value != 0) {
					volume = args.Volume.Value;
				}
				return ListFiles(args.LogPath, args.DatabasePath, volume, args.SelectedVolumeOnly, (x) => {
					bool minLimit = args.MinInstanceCount == null || x.Count >= args.MinInstanceCount.Value;
					bool maxLimit = args.MaxInstanceCount == null || x.Count <= args.MaxInstanceCount.Value;
					return minLimit && maxLimit;
				});
			} else {
				return ListVolumes(args.LogPath, args.DatabasePath, args.ShowDisabledVolumes);
			}
		}

		private static int ListVolumes(string logPath, string databasePath, bool showDisabledVolumes) {
			using (TextWriterWrapper textWriterWrapper = new TextWriterWrapper(logPath))
			using (SQLiteConnection connection = new SQLiteConnection("Data Source=" + databasePath)) {
				connection.Open();
				PrintVolumeInformation(textWriterWrapper.Writer, VolumeOperations.GetKnownVolumes(connection).Where(x => showDisabledVolumes || x.ShouldScan));
				connection.Close();
			}

			return 0;
		}

		private static string AsLetter(int i) {
			int a = i % 26;
			int b = i / 26;
			if (b != 0) {
				return AsLetter(b - 1) + AsLetter(a);
			}
			switch (a) {
				case 0: return "a";
				case 1: return "b";
				case 2: return "c";
				case 3: return "d";
				case 4: return "e";
				case 5: return "f";
				case 6: return "g";
				case 7: return "h";
				case 8: return "i";
				case 9: return "j";
				case 10: return "k";
				case 11: return "l";
				case 12: return "m";
				case 13: return "n";
				case 14: return "o";
				case 15: return "p";
				case 16: return "q";
				case 17: return "r";
				case 18: return "s";
				case 19: return "t";
				case 20: return "u";
				case 21: return "v";
				case 22: return "w";
				case 23: return "x";
				case 24: return "y";
				case 25: return "z";
				default: throw new Exception("should never happen");
			}
		}

		private static int RunInteractiveFileDeleteMode(string databasePath, long volumeId, ShouldPrint shouldPrint) {
			using (SQLiteConnection connection = new SQLiteConnection("Data Source=" + databasePath)) {
				connection.Open();

				Volume volume;
				{
					var volumes = VolumeOperations.FindAndInsertAttachedVolumes(connection);
					volume = volumes.FirstOrDefault((x) => x.ID == volumeId);
				}
				if (volume == null) {
					Console.WriteLine("Volume {0} is not attached.", volumeId);
					return -1;
				}


				List<StorageFile> files = GetKnownFilesOnVolume(connection, volumeId);
				List<List<StorageFile>> allfiles = CollectFiles(connection, files, shouldPrint, volumeId).ToList();
				for (int allfilesindex = 0; allfilesindex < allfiles.Count; allfilesindex++) {
					allfiles[allfilesindex] = allfiles[allfilesindex].OrderBy(x => (x.Path + "/" + x.Filename)).ToList();
				}

				int more = 5;
				for (int allfilesindex = 0; allfilesindex < allfiles.Count; allfilesindex++) {
					List<StorageFile> sameFiles = allfiles[allfilesindex];
					List<string> existingFilenames = new List<string>();
					foreach (StorageFile ssf in sameFiles) {
						if (!existingFilenames.Contains(ssf.Filename)) {
							existingFilenames.Add(ssf.Filename);
						}
					}
					existingFilenames.Sort();

					int? selectedFilename = null;
					while (true) {
						int prevfilesindex = allfilesindex - 1;
						int nextfilesindex = allfilesindex + more;
						if (prevfilesindex >= 0) {
							PrintFileInfoForInteractiveDelete(allfiles[prevfilesindex], "     ");
							Console.WriteLine();
						}
						PrintFileInfoForInteractiveDelete(sameFiles, " >>> ");

						if (existingFilenames.Count > 1) {
							Console.WriteLine(" >>> Available filenames:");
							for (int i = 0; i < existingFilenames.Count; ++i) {
								Console.WriteLine(" >>> {2}Filename {0}: {1}", AsLetter(i), existingFilenames[i], selectedFilename == i ? "!" : " ");
							}
						}

						for (int iiii = allfilesindex + 1; iiii <= nextfilesindex; ++iiii) {
							if (iiii < allfiles.Count) {
								Console.WriteLine();
								PrintFileInfoForInteractiveDelete(allfiles[iiii], "     ");
							}
						}

						Console.WriteLine();
						Console.WriteLine(" [file {0}/{1}, {2} to go]", allfilesindex + 1, allfiles.Count, allfiles.Count - allfilesindex);

						Console.WriteLine();
						Console.WriteLine("Enter number of file to keep, lowercase letter for target filename, nothing to skip, Q to quit, +/- to show more/less files.");
						Console.Write(" > ");

						string input = Console.ReadLine();
						if (input == "") {
							Console.Clear();
							break;
						}
						if (input == "+") {
							++more;
							Console.Clear();
							continue;
						}
						if (input == "-") {
							if (more > 0) {
								--more;
							}
							Console.Clear();
							continue;
						}
						if (input == "Q") {
							return -2;
						}

						int number;
						if (int.TryParse(input, out number)) {
							if (number >= 0 && number < sameFiles.Count) {
								List<(FileInfo fi, bool shouldDelete)> data = new List<(FileInfo fi, bool shouldDelete)>();
								for (int i = 0; i < sameFiles.Count; ++i) {
									var sf = sameFiles[i];
									string path;
									if (sf.Path == "" || sf.Path == "/" || sf.Path == "\\") {
										path = Path.Combine(volume.DeviceID, sf.Filename);
									} else if (sf.Path[0] == '/' || sf.Path[0] == '\\') {
										path = Path.Combine(volume.DeviceID, sf.Path.Substring(1).Replace('/', '\\'), sf.Filename);
									} else {
										Console.WriteLine("Unexpected path in database: " + sf.Path);
										break;
									}
									data.Add((new FileInfo(path), i != number));
								}

								Console.Clear();

								bool inconsistent = false;
								foreach (var d in data) {
									if (!d.fi.Exists) {
										Console.WriteLine("Inconsistent file state: File {0} does not actually exist on disk.", d.fi.FullName);
										inconsistent = true;
									}
								}
								if (!inconsistent) {
									foreach (var d in data) {
										if (d.shouldDelete) {
											Console.WriteLine("Deleting {0}", d.fi.FullName);
											d.fi.Delete();
										} else {
											if (selectedFilename.HasValue) {
												string newpath = Path.Combine(Path.GetDirectoryName(d.fi.FullName), existingFilenames[selectedFilename.Value]);
												if (File.Exists(newpath)) {
													Console.WriteLine("Keeping {0}; renaming to {1} not possible, skipping it", d.fi.FullName, newpath);
												} else {
													Console.WriteLine("Renaming {0} to {1}", d.fi.FullName, newpath);
													File.Move(d.fi.FullName, newpath);
												}
											} else {
												Console.WriteLine("Keeping {0}", d.fi.FullName);
											}
										}
									}
								}
								break;
							}
						}

						for (int i = 0; i < existingFilenames.Count; ++i) {
							if (AsLetter(i) == input) {
								selectedFilename = i;
								break;
							}
						}
					}
				}

				connection.Close();
			}

			return 0;
		}

		private static void PrintFileInfoForInteractiveDelete(List<StorageFile> sameFiles, string prefix) {
			Console.WriteLine(prefix + "File #{0}", sameFiles[0].FileId);
			Console.Write(prefix + "{0:N0} bytes / SHA256: ", sameFiles[0].Size);
			foreach (byte b in sameFiles[0].Hash) {
				Console.Write("{0:x2}", b);
			}
			Console.WriteLine();
			Console.WriteLine(prefix + "Exists in {0} places:", sameFiles.Count);
			for (int i = 0; i < sameFiles.Count; ++i) {
				var sf = sameFiles[i];
				Console.WriteLine(prefix + " No. {0}: {1}/{2}", i, sf.Path, sf.Filename);
			}
		}

		private static int ListFiles(string logPath, string databasePath, int? volume, bool selectedVolumeOnly, ShouldPrint shouldPrint) {
			using (TextWriterWrapper textWriterWrapper = new TextWriterWrapper(logPath))
			using (SQLiteConnection connection = new SQLiteConnection("Data Source=" + databasePath)) {
				connection.Open();

				List<StorageFile> files = GetKnownFilesOnVolume(connection, volume);
				List<Volume> volumes = VolumeOperations.GetKnownVolumes(connection);
				foreach (var sameFiles in CollectFiles(connection, files, shouldPrint, selectedVolumeOnly ? volume : null)) {
					PrintSameFileInformation(textWriterWrapper.Writer, sameFiles, volumes);
				}

				connection.Close();
			}

			return 0;
		}

		private static int Search(SearchOptions args) {
			using (TextWriterWrapper textWriterWrapper = new TextWriterWrapper(args.LogPath))
			using (SQLiteConnection connection = new SQLiteConnection("Data Source=" + args.DatabasePath)) {
				connection.Open();
				List<Volume> volumes = VolumeOperations.GetKnownVolumes(connection);
				foreach (var sameFiles in CollectFiles(connection, GetFilesWithFilename(connection, "%" + args.File + "%"), (x) => true)) {
					PrintSameFileInformation(textWriterWrapper.Writer, sameFiles, volumes);
				}
				connection.Close();
			}

			return 0;
		}

		private static void PrintVolumeInformation(TextWriter stdout, IEnumerable<Volume> volumes) {
			foreach (var volume in volumes.OrderBy(x => x.Label)) {
				string lastScan = volume.LastScan.ToString("u");
				stdout.WriteLine(
					"{6}Volume #{0,3}: {1,-40} [{2,19:N0} free / {3,19:N0} total] (last scan: {4}) {5}",
					volume.ID,
					volume.Label,
					volume.FreeSpace,
					volume.TotalSpace,
					lastScan,
					volume.Dirty != 0 ? "(dirty)" : "",
					volume.ShouldScan ? "  " : "D "
				);
			}
		}

		private static void PrintSameFileInformation(TextWriter stdout, List<StorageFile> sameFiles, List<Volume> volumes) {
			if (sameFiles.Count > 0) {
				stdout.WriteLine("File #{0}", sameFiles[0].FileId);
				stdout.Write("{0:N0} bytes / SHA256: ", sameFiles[0].Size);
				foreach (byte b in sameFiles[0].Hash) {
					stdout.Write("{0:x2}", b);
				}
				stdout.WriteLine();
				stdout.WriteLine("Exists in {0} places:", sameFiles.Count);
				foreach (var sf in sameFiles) {
					Volume vol = volumes.Where(x => x.ID == sf.VolumeId).FirstOrDefault();
					stdout.WriteLine("  Volume #{0} [{1}], {2}/{3}", sf.VolumeId, vol != null ? vol.Label : "?", sf.Path, sf.Filename);
				}
				stdout.WriteLine();
			}
		}

		private delegate bool ShouldPrint(List<StorageFile> files);
		private static IEnumerable<List<StorageFile>> CollectFiles(SQLiteConnection connection, List<StorageFile> files, ShouldPrint shouldPrint, long? onlyOnVolume = null) {
			ISet<long> seenIds = new HashSet<long>();
			foreach (var file in files) {
				if (seenIds.Contains(file.FileId)) {
					continue;
				}

				seenIds.Add(file.FileId);
				var sameFiles = GetStorageFilesForFileId(connection, file.FileId, onlyOnVolume);
				if (shouldPrint(sameFiles)) {
					yield return sameFiles;
				}
			}
		}

		private static List<StorageFile> GetStorageFilesForFileId(SQLiteConnection connection, long fileId, long? onlyOnVolume = null) {
			var rv = HyoutaTools.SqliteUtil.SelectArray(connection,
				"SELECT Files.size, Files.shorthash, Files.hash, Storage.id AS storageId, Pathnames.name AS pathname, " +
				"Paths.volumeId, Filenames.name AS filename, Storage.timestamp, Storage.lastSeen " +
				"FROM Files " +
				"INNER JOIN Storage ON Files.id = Storage.fileId " +
				"INNER JOIN Paths ON Storage.pathId = Paths.id " +
				"INNER JOIN Pathnames ON Paths.pathnameId = Pathnames.id " +
				"INNER JOIN Filenames ON Storage.filenameId = Filenames.id " +
				"WHERE Files.id = ?" + (onlyOnVolume != null ? " AND Paths.volumeId = ?" : ""),
				onlyOnVolume != null ? new object[] { fileId, onlyOnVolume.Value } : new object[] { fileId }
			);

			if (rv == null || rv.Count == 0) {
				return new List<StorageFile>();
			}

			List<StorageFile> files = new List<StorageFile>(rv.Count);
			foreach (var arr in rv) {
				files.Add(new StorageFile() {
					FileId = fileId,
					Size = (long)arr[0],
					ShortHash = (byte[])arr[1],
					Hash = (byte[])arr[2],
					StorageId = (long)arr[3],
					Path = (string)arr[4],
					VolumeId = (long)arr[5],
					Filename = (string)arr[6],
					Timestamp = HyoutaTools.Util.UnixTimeToDateTime((long)arr[7]),
					LastSeen = HyoutaTools.Util.UnixTimeToDateTime((long)arr[8]),
				});
			}

			return files;
		}

		private static List<StorageFile> GetKnownFilesOnVolume(SQLiteConnection connection, long? volumeId) {
			string statement = "SELECT Files.id, Files.size, Files.shorthash, Files.hash, Storage.id AS storageId, " +
				"Pathnames.name AS pathname, Filenames.name AS filename, Storage.timestamp, Storage.lastSeen, Paths.volumeId AS volumeId " +
				"FROM Storage " +
				"INNER JOIN Files ON Storage.fileId = Files.id " +
				"INNER JOIN Paths ON Storage.pathId = Paths.id " +
				"INNER JOIN Pathnames ON Paths.pathnameId = Pathnames.id " +
				"INNER JOIN Filenames ON Storage.filenameId = Filenames.id";

			List<object[]> rv;
			if (volumeId.HasValue) {
				rv = HyoutaTools.SqliteUtil.SelectArray(connection, statement + " WHERE Paths.volumeId = ?", new object[] { volumeId.Value });
			} else {
				rv = HyoutaTools.SqliteUtil.SelectArray(connection, statement, new object[0]);
			}

			if (rv == null || rv.Count == 0) {
				return new List<StorageFile>();
			}

			List<StorageFile> files = new List<StorageFile>(rv.Count);
			foreach (var arr in rv) {
				files.Add(new StorageFile() {
					FileId = (long)arr[0],
					Size = (long)arr[1],
					ShortHash = (byte[])arr[2],
					Hash = (byte[])arr[3],
					StorageId = (long)arr[4],
					Path = (string)arr[5],
					Filename = (string)arr[6],
					VolumeId = (long)arr[9],
					Timestamp = HyoutaTools.Util.UnixTimeToDateTime((long)arr[7]),
					LastSeen = HyoutaTools.Util.UnixTimeToDateTime((long)arr[8]),
				});
			}

			return files;
		}

		private static List<StorageFile> GetFilesWithFilename(SQLiteConnection connection, string filename) {
			var rv = HyoutaTools.SqliteUtil.SelectArray(connection,
				"SELECT Files.id, Files.size, Files.shorthash, Files.hash, Storage.id AS storageId, Paths.volumeId, " +
				"Pathnames.name AS pathname, Filenames.name AS filename, Storage.timestamp, Storage.lastSeen " +
				"FROM Storage " +
				"INNER JOIN Files ON Storage.fileId = Files.id " +
				"INNER JOIN Paths ON Storage.pathId = Paths.id " +
				"INNER JOIN Pathnames ON Paths.pathnameId = Pathnames.id " +
				"INNER JOIN Filenames ON Storage.filenameId = Filenames.id " +
				"WHERE Filenames.name LIKE ?", new object[] { filename });

			if (rv == null || rv.Count == 0) {
				return new List<StorageFile>();
			}

			List<StorageFile> files = new List<StorageFile>(rv.Count);
			foreach (var arr in rv) {
				files.Add(new StorageFile() {
					FileId = (long)arr[0],
					Size = (long)arr[1],
					ShortHash = (byte[])arr[2],
					Hash = (byte[])arr[3],
					StorageId = (long)arr[4],
					VolumeId = (long)arr[5],
					Path = (string)arr[6],
					Filename = (string)arr[7],
					Timestamp = HyoutaTools.Util.UnixTimeToDateTime((long)arr[8]),
					LastSeen = HyoutaTools.Util.UnixTimeToDateTime((long)arr[9]),
				});
			}

			return files;
		}

		private static List<StorageFile> ProcessVolume(TextWriter stdout, SQLiteConnection connection, Volume volume) {
			if (!volume.ShouldScan) {
				return null;
			}

			using (IDbTransaction t = connection.BeginTransaction()) {
				HyoutaTools.SqliteUtil.Update(t, "UPDATE Volumes SET dirty = 1 WHERE id = ?", new object[] { volume.ID });
				t.Commit();
			}

			List<StorageFile> files = new List<StorageFile>();
			ProcessDirectory(stdout, connection, files, volume, new DirectoryInfo(volume.DeviceID), "");
			DiscardUnseenStorageFiles(stdout, connection, files, volume);

			using (IDbTransaction t = connection.BeginTransaction()) {
				long lastScan = HyoutaTools.Util.DateTimeToUnixTime(DateTime.UtcNow);
				HyoutaTools.SqliteUtil.Update(t, "UPDATE Volumes SET dirty = 0, lastScan = ? WHERE id = ?", new object[] { lastScan, volume.ID });
				t.Commit();
			}

			return files;
		}

		private static void DiscardUnseenStorageFiles(TextWriter stdout, SQLiteConnection connection, List<StorageFile> files, Volume volume) {
			var knownFiles = GetKnownFilesOnVolume(connection, volume.ID);
			ISet<long> seenStorageIds = new HashSet<long>();
			foreach (var f in files) {
				seenStorageIds.Add(f.StorageId);
			}
			using (IDbTransaction t = connection.BeginTransaction()) {
				foreach (var f in knownFiles) {
					if (!seenStorageIds.Contains(f.StorageId)) {
						stdout.WriteLine("[" + volume.Label + "] Discarding unseen file {0}/{1}", f.Path, f.Filename);
						HyoutaTools.SqliteUtil.Update(t, "DELETE FROM Storage WHERE id = ?", new object[] { f.StorageId });
					}
				}
				t.Commit();
			}
		}

		private static void ProcessDirectory(TextWriter stdout, SQLiteConnection connection, List<StorageFile> files, Volume volume, DirectoryInfo directory, string path) {
			try {
				foreach (var fsi in directory.GetFileSystemInfos()) {
					if (fsi is FileInfo) {
						ProcessFile(stdout, connection, files, volume, fsi as FileInfo, path);
					} else if (fsi is DirectoryInfo) {
						ProcessDirectory(stdout, connection, files, volume, fsi as DirectoryInfo, path + "/" + fsi.Name);
					}
				}
			} catch (UnauthorizedAccessException ex) {
				stdout.WriteLine(ex.ToString());
			}
		}

		private static void ProcessFile(TextWriter stdout, SQLiteConnection connection, List<StorageFile> files, Volume volume, FileInfo file, string dirPath) {
			try {
				stdout.Write("[" + volume.Label + "] Checking file: " + dirPath + "/" + file.Name + ", " + string.Format("{0:n0}", file.Length) + " bytes...");
				byte[] shorthash;
				using (var fs = new FileStream(file.FullName, FileMode.Open, FileAccess.Read)) {
					shorthash = HashUtil.CalculateShortHash(fs);
				}
				StorageFile sf = CheckAndUpdateFile(connection, volume, file, dirPath, shorthash);
				if (sf != null) {
					stdout.WriteLine(" seems same.");
					files.Add(sf);
					return;
				}
				stdout.WriteLine(" is different or new.");

				long filesize;
				byte[] hash;
				using (var fs = new FileStream(file.FullName, FileMode.Open, FileAccess.Read)) {
					filesize = fs.Length;
					shorthash = HashUtil.CalculateShortHash(fs);
					hash = HashUtil.CalculateHash(fs);
				}
				files.Add(InsertOrUpdateFileAndStorage(connection, volume, dirPath, file.Name, filesize, hash, shorthash, file.LastWriteTimeUtc));
			} catch (UnauthorizedAccessException ex) {
				stdout.WriteLine(ex.ToString());
			}
		}

		private static StorageFile InsertOrUpdateFileAndStorage(SQLiteConnection connection, Volume volume, string dirPath, string name, long filesize, byte[] hash, byte[] shorthash, DateTime lastWriteTimeUtc) {
			using (IDbTransaction t = connection.BeginTransaction()) {
				long timestamp = HyoutaTools.Util.DateTimeToUnixTime(lastWriteTimeUtc);
				long fileId = FileOperations.InsertOrGetFile(t, filesize, hash, shorthash, lastWriteTimeUtc).fileId;
				long pathId = DatabaseHelper.InsertOrUpdatePath(t, volume.ID, dirPath);
				long filenameId = DatabaseHelper.InsertOrUpdateFilename(t, name);

				var rv = HyoutaTools.SqliteUtil.SelectScalar(t, "SELECT id FROM Storage WHERE pathId = ? AND filenameId = ?", new object[] { pathId, filenameId });
				long lastSeen = HyoutaTools.Util.DateTimeToUnixTime(DateTime.UtcNow);
				long storageId;
				if (rv == null) {
					HyoutaTools.SqliteUtil.Update(t, "INSERT INTO Storage ( fileId, pathId, filenameId, timestamp, lastSeen ) VALUES ( ?, ?, ?, ?, ? )", new object[] { fileId, pathId, filenameId, timestamp, lastSeen });
					rv = HyoutaTools.SqliteUtil.SelectScalar(t, "SELECT id FROM Storage WHERE pathId = ? AND filenameId = ?", new object[] { pathId, filenameId });
					storageId = (long)rv;
				} else {
					storageId = (long)rv;
					HyoutaTools.SqliteUtil.Update(t, "UPDATE Storage SET fileId = ?, timestamp = ?, lastSeen = ? WHERE id = ?", new object[] { fileId, timestamp, lastSeen, storageId });
				}

				t.Commit();

				return new StorageFile() {
					FileId = fileId,
					Size = filesize,
					Hash = hash,
					ShortHash = shorthash,
					StorageId = storageId,
					Path = dirPath,
					Filename = name,
					VolumeId = volume.ID,
					Timestamp = HyoutaTools.Util.UnixTimeToDateTime(timestamp),
					LastSeen = HyoutaTools.Util.UnixTimeToDateTime(lastSeen),
				};
			}
		}

		private static StorageFile CheckAndUpdateFile(SQLiteConnection connection, Volume volume, FileInfo file, string dirPath, byte[] expectedShorthash) {
			using (IDbTransaction t = connection.BeginTransaction()) {
				var rv = HyoutaTools.SqliteUtil.SelectArray(t, "SELECT Storage.id, Files.size, Storage.timestamp, Files.shorthash, Storage.fileId FROM Storage " +
					"INNER JOIN Files ON Storage.fileId = Files.id " +
					"INNER JOIN Paths ON Storage.pathId = Paths.id " +
					"INNER JOIN Pathnames ON Paths.pathnameId = Pathnames.id " +
					"INNER JOIN Filenames ON Storage.filenameId = Filenames.id " +
					"WHERE Paths.volumeId = ? AND Pathnames.name = ? AND Filenames.name = ?", new object[] { volume.ID, dirPath, file.Name });
				if (rv == null || rv.Count == 0) {
					return null;
				}

				long storageId = (long)rv[0][0];
				long fileSize = (long)rv[0][1];
				long timestamp = (long)rv[0][2];
				byte[] shorthash = (byte[])rv[0][3];
				long fileId = (long)rv[0][4];

				long expectedFilesize = file.Length;
				long expectedTimestamp = HyoutaTools.Util.DateTimeToUnixTime(file.LastWriteTimeUtc);

				if (fileSize != expectedFilesize || timestamp != expectedTimestamp || !expectedShorthash.SequenceEqual(shorthash)) {
					return null;
				}

				// seems to check out
				long updateTimestamp = HyoutaTools.Util.DateTimeToUnixTime(DateTime.UtcNow);
				HyoutaTools.SqliteUtil.Update(t, "UPDATE Storage SET lastSeen = ? WHERE id = ?", new object[] { updateTimestamp, storageId });
				t.Commit();

				return new StorageFile() {
					FileId = fileId,
					Size = fileSize,
					ShortHash = shorthash,
					StorageId = storageId,
					VolumeId = volume.ID,
					Path = dirPath,
					Filename = file.Name,
					Timestamp = HyoutaTools.Util.UnixTimeToDateTime(timestamp),
					LastSeen = HyoutaTools.Util.UnixTimeToDateTime(updateTimestamp),
				};
			}
		}
	}
}
