using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Management;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace DiskFileManager {
	class Volume {
		public long ID;
		public string DeviceID;
		public string Label;
		public bool ShouldScan;
	}

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

	class Program {
		static void Main( string[] args ) {
			string databaseFilePath = args[0];
			using ( SQLiteConnection connection = new SQLiteConnection( "Data Source=" + databaseFilePath ) ) {
				connection.Open();
				CreateTables( connection );

				long? volIdCheck = null;
				if ( volIdCheck != null ) {
					PrintFileInformation( connection, GetKnownFilesOnVolume( connection, volIdCheck.Value ) );
					return;
				}

				List<Volume> volumes = new List<Volume>();
				foreach ( ManagementObject vol in new ManagementClass( "Win32_Volume" ).GetInstances() ) {
					string id = vol.Properties["DeviceID"].Value.ToString();
					string label = vol?.Properties["Label"]?.Value?.ToString() ?? "";
					volumes.Add( CreateOrFindVolume( connection, id, label ) );
				}

				foreach ( Volume v in volumes ) {
					ProcessVolume( connection, v );
				}

				connection.Close();
			}

			return;
		}

		private static void PrintFileInformation( SQLiteConnection connection, List<StorageFile> files ) {
			foreach ( var file in files ) {
				var sameFiles = GetStorageFilesForFileId( connection, file.FileId );
				Console.WriteLine( "File #{0}", file.FileId );
				Console.WriteLine( "{0:N0} bytes", file.Size );
				Console.WriteLine( "Exists in {0} places:", sameFiles.Count );
				foreach ( var sf in sameFiles ) {
					Console.WriteLine( "  Volume #{0}, {1}/{2}", sf.VolumeId, sf.Path, sf.Filename );
				}
				Console.WriteLine();
			}
		}

		private static List<StorageFile> GetStorageFilesForFileId( SQLiteConnection connection, long fileId ) {
			var rv = HyoutaTools.SqliteUtil.SelectArray( connection,
				"SELECT Files.size, Files.shorthash, Files.hash, Storage.id AS storageId, Pathnames.name AS pathname, " +
				"Paths.volumeId, Filenames.name AS filename, Storage.timestamp, Storage.lastSeen " +
				"FROM Files " +
				"INNER JOIN Storage ON Files.id = Storage.fileId " +
				"INNER JOIN Paths ON Storage.pathId = Paths.id " +
				"INNER JOIN Pathnames ON Paths.pathnameId = Pathnames.id " +
				"INNER JOIN Filenames ON Storage.filenameId = Filenames.id " +
				"WHERE Files.id = ?", new object[] { fileId } );

			if ( rv == null || rv.Count == 0 ) {
				return new List<StorageFile>();
			}

			List<StorageFile> files = new List<StorageFile>( rv.Count );
			foreach ( var arr in rv ) {
				files.Add( new StorageFile() {
					FileId = fileId,
					Size = (long)arr[0],
					ShortHash = (byte[])arr[1],
					Hash = (byte[])arr[2],
					StorageId = (long)arr[3],
					Path = (string)arr[4],
					VolumeId = (long)arr[5],
					Filename = (string)arr[6],
					Timestamp = HyoutaTools.Util.UnixTimeToDateTime( (ulong)( (long)arr[7] ) ),
					LastSeen = HyoutaTools.Util.UnixTimeToDateTime( (ulong)( (long)arr[8] ) ),
				} );
			}

			return files;
		}

		private static List<StorageFile> GetKnownFilesOnVolume( SQLiteConnection connection, long volumeId ) {
			var rv = HyoutaTools.SqliteUtil.SelectArray( connection,
				"SELECT Files.id, Files.size, Files.shorthash, Files.hash, Storage.id AS storageId, " +
				"Pathnames.name AS pathname, Filenames.name AS filename, Storage.timestamp, Storage.lastSeen " +
				"FROM Storage " +
				"INNER JOIN Files ON Storage.fileId = Files.id " +
				"INNER JOIN Paths ON Storage.pathId = Paths.id " +
				"INNER JOIN Pathnames ON Paths.pathnameId = Pathnames.id " +
				"INNER JOIN Filenames ON Storage.filenameId = Filenames.id " +
				"WHERE Paths.volumeId = ?", new object[] { volumeId } );

			if ( rv == null || rv.Count == 0 ) {
				return new List<StorageFile>();
			}

			List<StorageFile> files = new List<StorageFile>( rv.Count );
			foreach ( var arr in rv ) {
				files.Add( new StorageFile() {
					FileId = (long)arr[0],
					Size = (long)arr[1],
					ShortHash = (byte[])arr[2],
					Hash = (byte[])arr[3],
					StorageId = (long)arr[4],
					Path = (string)arr[5],
					Filename = (string)arr[6],
					VolumeId = volumeId,
					Timestamp = HyoutaTools.Util.UnixTimeToDateTime( (ulong)( (long)arr[7] ) ),
					LastSeen = HyoutaTools.Util.UnixTimeToDateTime( (ulong)( (long)arr[8] ) ),
				} );
			}

			return files;
		}

		private static List<StorageFile> ProcessVolume( SQLiteConnection connection, Volume volume ) {
			if ( !volume.ShouldScan ) {
				return null;
			}

			List<StorageFile> files = new List<StorageFile>();
			ProcessDirectory( connection, files, volume, new DirectoryInfo( volume.DeviceID ), "" );
			DiscardUnseenStorageFiles( connection, files, volume );
			return files;
		}

		private static void DiscardUnseenStorageFiles( SQLiteConnection connection, List<StorageFile> files, Volume volume ) {
			var knownFiles = GetKnownFilesOnVolume( connection, volume.ID );
			ISet<long> seenStorageIds = new HashSet<long>();
			foreach ( var f in files ) {
				seenStorageIds.Add( f.StorageId );
			}
			using ( IDbTransaction t = connection.BeginTransaction() ) {
				foreach ( var f in knownFiles ) {
					if ( !seenStorageIds.Contains( f.StorageId ) ) {
						Console.WriteLine( "[" + volume.Label + "] Discarding unseen file {0}/{1}", f.Path, f.Filename );
						HyoutaTools.SqliteUtil.Update( t, "DELETE FROM Storage WHERE id = ?", new object[] { f.StorageId } );
					}
				}
				t.Commit();
			}
		}

		private static void ProcessDirectory( SQLiteConnection connection, List<StorageFile> files, Volume volume, DirectoryInfo directory, string path ) {
			try {
				foreach ( var fsi in directory.GetFileSystemInfos() ) {
					if ( fsi is FileInfo ) {
						ProcessFile( connection, files, volume, fsi as FileInfo, path );
					} else if ( fsi is DirectoryInfo ) {
						ProcessDirectory( connection, files, volume, fsi as DirectoryInfo, path + "/" + fsi.Name );
					}
				}
			} catch ( UnauthorizedAccessException ex ) {
				Console.WriteLine( ex.ToString() );
			}
		}

		private static void ProcessFile( SQLiteConnection connection, List<StorageFile> files, Volume volume, FileInfo file, string dirPath ) {
			try {
				Console.Write( "[" + volume.Label + "] Checking file: " + dirPath + "/" + file.Name + ", " + string.Format( "{0:n0}", file.Length ) + " bytes..." );
				byte[] shorthash;
				using ( var fs = new FileStream( file.FullName, FileMode.Open, FileAccess.Read ) ) {
					shorthash = HashUtil.CalculateShortHash( fs );
				}
				StorageFile sf = CheckAndUpdateFile( connection, volume, file, dirPath, shorthash );
				if ( sf != null ) {
					Console.WriteLine( " seems same." );
					files.Add( sf );
					return;
				}
				Console.WriteLine( " is different or new." );

				long filesize;
				byte[] hash;
				using ( var fs = new FileStream( file.FullName, FileMode.Open, FileAccess.Read ) ) {
					filesize = fs.Length;
					shorthash = HashUtil.CalculateShortHash( fs );
					hash = HashUtil.CalculateHash( fs );
				}
				files.Add( InsertOrUpdateFile( connection, volume, dirPath, file.Name, filesize, hash, shorthash, file.LastWriteTimeUtc ) );
			} catch ( UnauthorizedAccessException ex ) {
				Console.WriteLine( ex.ToString() );
			}
		}

		private static long InsertOrUpdateFilename( IDbTransaction t, string name ) {
			var rv = HyoutaTools.SqliteUtil.SelectScalar( t, "SELECT id FROM Filenames WHERE name = ?", new object[] { name } );
			if ( rv == null ) {
				HyoutaTools.SqliteUtil.Update( t, "INSERT INTO Filenames ( name ) VALUES ( ? )", new object[] { name } );
				rv = HyoutaTools.SqliteUtil.SelectScalar( t, "SELECT id FROM Filenames WHERE name = ?", new object[] { name } );
			}
			return (long)rv;
		}

		private static long InsertOrUpdatePathname( IDbTransaction t, string name ) {
			var rv = HyoutaTools.SqliteUtil.SelectScalar( t, "SELECT id FROM Pathnames WHERE name = ?", new object[] { name } );
			if ( rv == null ) {
				HyoutaTools.SqliteUtil.Update( t, "INSERT INTO Pathnames ( name ) VALUES ( ? )", new object[] { name } );
				rv = HyoutaTools.SqliteUtil.SelectScalar( t, "SELECT id FROM Pathnames WHERE name = ?", new object[] { name } );
			}
			return (long)rv;
		}

		private static long InsertOrUpdatePath( IDbTransaction t, long volumeId, string name ) {
			long pathnameId = InsertOrUpdatePathname( t, name );
			var rv = HyoutaTools.SqliteUtil.SelectScalar( t, "SELECT id FROM Paths WHERE volumeId = ? AND pathnameId = ?", new object[] { volumeId, pathnameId } );
			if ( rv == null ) {
				HyoutaTools.SqliteUtil.Update( t, "INSERT INTO Paths ( volumeId, pathnameId ) VALUES ( ?, ? )", new object[] { volumeId, pathnameId } );
				rv = HyoutaTools.SqliteUtil.SelectScalar( t, "SELECT id FROM Paths WHERE volumeId = ? AND pathnameId = ?", new object[] { volumeId, pathnameId } );
			}
			return (long)rv;
		}

		private static StorageFile InsertOrUpdateFile( SQLiteConnection connection, Volume volume, string dirPath, string name, long filesize, byte[] hash, byte[] shorthash, DateTime lastWriteTimeUtc ) {
			using ( IDbTransaction t = connection.BeginTransaction() ) {
				var rv = HyoutaTools.SqliteUtil.SelectScalar( t, "SELECT id FROM Files WHERE size = ? AND hash = ? AND shorthash = ?", new object[] { filesize, hash, shorthash } );
				if ( rv == null ) {
					HyoutaTools.SqliteUtil.Update( t, "INSERT INTO Files ( size, hash, shorthash ) VALUES ( ?, ?, ? )", new object[] { filesize, hash, shorthash } );
					rv = HyoutaTools.SqliteUtil.SelectScalar( t, "SELECT id FROM Files WHERE size = ? AND hash = ? AND shorthash = ?", new object[] { filesize, hash, shorthash } );
				}

				long fileId = (long)rv;
				long pathId = InsertOrUpdatePath( t, volume.ID, dirPath );
				long filenameId = InsertOrUpdateFilename( t, name );

				rv = HyoutaTools.SqliteUtil.SelectScalar( t, "SELECT id FROM Storage WHERE pathId = ? AND filenameId = ?", new object[] { pathId, filenameId } );
				long timestamp = (long)HyoutaTools.Util.DateTimeToUnixTime( lastWriteTimeUtc );
				long lastSeen = (long)HyoutaTools.Util.DateTimeToUnixTime( DateTime.UtcNow );
				long storageId;
				if ( rv == null ) {
					HyoutaTools.SqliteUtil.Update( t, "INSERT INTO Storage ( fileId, pathId, filenameId, timestamp, lastSeen ) VALUES ( ?, ?, ?, ?, ? )", new object[] { fileId, pathId, filenameId, timestamp, lastSeen } );
					rv = HyoutaTools.SqliteUtil.SelectScalar( t, "SELECT id FROM Storage WHERE pathId = ? AND filenameId = ?", new object[] { pathId, filenameId } );
					storageId = (long)rv;
				} else {
					storageId = (long)rv;
					HyoutaTools.SqliteUtil.Update( t, "UPDATE Storage SET fileId = ?, timestamp = ?, lastSeen = ? WHERE id = ?", new object[] { fileId, timestamp, lastSeen, storageId } );
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
					Timestamp = HyoutaTools.Util.UnixTimeToDateTime( (ulong)timestamp ),
					LastSeen = HyoutaTools.Util.UnixTimeToDateTime( (ulong)lastSeen ),
				};
			}
		}

		private static StorageFile CheckAndUpdateFile( SQLiteConnection connection, Volume volume, FileInfo file, string dirPath, byte[] expectedShorthash ) {
			using ( IDbTransaction t = connection.BeginTransaction() ) {
				var rv = HyoutaTools.SqliteUtil.SelectArray( t, "SELECT Storage.id, Files.size, Storage.timestamp, Files.shorthash, Storage.fileId FROM Storage " +
					"INNER JOIN Files ON Storage.fileId = Files.id " +
					"INNER JOIN Paths ON Storage.pathId = Paths.id " +
					"INNER JOIN Pathnames ON Paths.pathnameId = Pathnames.id " +
					"INNER JOIN Filenames ON Storage.filenameId = Filenames.id " +
					"WHERE Paths.volumeId = ? AND Pathnames.name = ? AND Filenames.name = ?", new object[] { volume.ID, dirPath, file.Name } );
				if ( rv == null || rv.Count == 0 ) {
					return null;
				}

				long storageId = (long)rv[0][0];
				long fileSize = (long)rv[0][1];
				long timestamp = (long)rv[0][2];
				byte[] shorthash = (byte[])rv[0][3];
				long fileId = (long)rv[0][4];

				long expectedFilesize = file.Length;
				long expectedTimestamp = (long)HyoutaTools.Util.DateTimeToUnixTime( file.LastWriteTimeUtc );

				if ( fileSize != expectedFilesize || timestamp != expectedTimestamp || !expectedShorthash.SequenceEqual( shorthash ) ) {
					return null;
				}

				// seems to check out
				long updateTimestamp = (long)HyoutaTools.Util.DateTimeToUnixTime( DateTime.UtcNow );
				HyoutaTools.SqliteUtil.Update( t, "UPDATE Storage SET lastSeen = ? WHERE id = ?", new object[] { updateTimestamp, storageId } );
				t.Commit();

				return new StorageFile() {
					FileId = fileId,
					Size = fileSize,
					ShortHash = shorthash,
					StorageId = storageId,
					VolumeId = volume.ID,
					Path = dirPath,
					Filename = file.Name,
					Timestamp = HyoutaTools.Util.UnixTimeToDateTime( (ulong)timestamp ),
					LastSeen = HyoutaTools.Util.UnixTimeToDateTime( (ulong)updateTimestamp ),
				};
			}
		}

		private static Volume CreateOrFindVolume( SQLiteConnection connection, string id, string label ) {
			using ( IDbTransaction t = connection.BeginTransaction() ) {
				List<object[]> rv = HyoutaTools.SqliteUtil.SelectArray( t, "SELECT id, shouldScan FROM Volumes WHERE guid = ?", new object[] { id } );
				if ( rv == null || rv.Count == 0 ) {
					HyoutaTools.SqliteUtil.Update( t, "INSERT INTO Volumes ( guid, label, shouldScan ) VALUES ( ?, ?, ? )", new object[] { id, label, true } );
					rv = HyoutaTools.SqliteUtil.SelectArray( t, "SELECT id, shouldScan FROM Volumes WHERE guid = ?", new object[] { id } );
					t.Commit();
				}
				return new Volume() { DeviceID = id, Label = label, ID = (long)rv[0][0], ShouldScan = (bool)rv[0][1] };
			}
		}

		private static void CreateTables( SQLiteConnection connection ) {
			using ( IDbTransaction t = connection.BeginTransaction() ) {
				HyoutaTools.SqliteUtil.Update( t, "CREATE TABLE IF NOT EXISTS Files (" +
					"id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, " +
					"size INTERGER NOT NULL, " +
					"shorthash BLOB NOT NULL, " +
					"hash BLOB NOT NULL, " +
					"UNIQUE(size, shorthash, hash)" +
				")" );
				HyoutaTools.SqliteUtil.Update( t, "CREATE TABLE IF NOT EXISTS Volumes (" +
					"id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, " +
					"guid TEXT NOT NULL, " +
					"label TEXT NOT NULL, " +
					"shouldScan BOOLEAN NOT NULL" +
				")" );
				HyoutaTools.SqliteUtil.Update( t, "CREATE TABLE IF NOT EXISTS Pathnames (" +
					"id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, " +
					"name TEXT NOT NULL, " + // directory path, unix separator, no drive letter; no filename
					"UNIQUE(name)" +
				")" );
				HyoutaTools.SqliteUtil.Update( t, "CREATE TABLE IF NOT EXISTS Filenames (" +
					"id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, " +
					"name TEXT NOT NULL, " + // filename
					"UNIQUE(name)" +
				")" );
				HyoutaTools.SqliteUtil.Update( t, "CREATE TABLE IF NOT EXISTS Paths (" +
					"id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, " +
					"volumeId INTEGER NOT NULL, " +
					"pathnameId INTEGER NOT NULL, " +
					"FOREIGN KEY(volumeId) REFERENCES Volumes(id), " +
					"FOREIGN KEY(pathnameId) REFERENCES Pathnames(id), " +
					"UNIQUE(volumeId, pathnameId)" +
				")" );
				HyoutaTools.SqliteUtil.Update( t, "CREATE TABLE IF NOT EXISTS Storage (" +
					"id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, " +
					"fileId INTEGER NOT NULL, " +
					"pathId INTEGER NOT NULL, " +
					"filenameId INTEGER NOT NULL, " +
					"timestamp INTEGER NOT NULL, " + // actual on-disk last modified timestamp
					"lastSeen INTEGER NOT NULL, " + // when we have last seen it
					"FOREIGN KEY(fileId) REFERENCES Files(id), " +
					"FOREIGN KEY(pathId) REFERENCES Paths(id), " +
					"FOREIGN KEY(filenameId) REFERENCES Filenames(id), " +
					"UNIQUE(pathId, filenameId)" +
				")" );
				HyoutaTools.SqliteUtil.Update( t, "CREATE INDEX IF NOT EXISTS StorageFileId ON Storage (fileId)" );
				t.Commit();
			}
		}
	}
}
