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

	class Program {
		static void Main( string[] args ) {
			string databaseFilePath = args[0];
			using ( SQLiteConnection connection = new SQLiteConnection( "Data Source=" + databaseFilePath ) ) {
				connection.Open();
				CreateTables( connection );

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

		private static void ProcessVolume( SQLiteConnection connection, Volume volume ) {
			if ( !volume.ShouldScan ) {
				return;
			}

			ProcessDirectory( connection, volume, new DirectoryInfo( volume.DeviceID ), "" );
		}

		private static void ProcessDirectory( SQLiteConnection connection, Volume volume, DirectoryInfo directory, string path ) {
			try {
				foreach ( var fsi in directory.GetFileSystemInfos() ) {
					if ( fsi is FileInfo ) {
						ProcessFile( connection, volume, fsi as FileInfo, path );
					} else if ( fsi is DirectoryInfo ) {
						ProcessDirectory( connection, volume, fsi as DirectoryInfo, path + "/" + fsi.Name );
					}
				}
			} catch ( UnauthorizedAccessException ex ) {
				Console.WriteLine( ex.ToString() );
			}
		}

		private static void ProcessFile( SQLiteConnection connection, Volume volume, FileInfo file, string dirPath ) {
			try {
				Console.Write( "[" + volume.Label + "] Checking file: " + dirPath + "/" + file.Name + ", " + string.Format( "{0:n0}", file.Length ) + " bytes..." );
				byte[] shorthash;
				using ( var fs = new FileStream( file.FullName, FileMode.Open, FileAccess.Read ) ) {
					shorthash = HashUtil.CalculateShortHash( fs );
				}
				if ( CheckAndUpdateFile( connection, volume, file, dirPath, shorthash ) ) {
					Console.WriteLine( " seems same." );
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
				InsertOrUpdateFile( connection, volume, dirPath, file.Name, filesize, hash, shorthash, file.LastWriteTimeUtc );
			} catch ( UnauthorizedAccessException ex ) {
				Console.WriteLine( ex.ToString() );
			}
		}

		private static void InsertOrUpdateFile( SQLiteConnection connection, Volume volume, string dirPath, string name, long filesize, byte[] hash, byte[] shorthash, DateTime lastWriteTimeUtc ) {
			using ( IDbTransaction t = connection.BeginTransaction() ) {
				var rv = HyoutaTools.SqliteUtil.SelectScalar( t, "SELECT id FROM Files WHERE size = ? AND hash = ? AND shorthash = ?", new object[] { filesize, hash, shorthash } );
				if ( rv == null ) {
					HyoutaTools.SqliteUtil.Update( t, "INSERT INTO Files ( size, hash, shorthash ) VALUES ( ?, ?, ? )", new object[] { filesize, hash, shorthash } );
					rv = HyoutaTools.SqliteUtil.SelectScalar( t, "SELECT id FROM Files WHERE size = ? AND hash = ? AND shorthash = ?", new object[] { filesize, hash, shorthash } );
				}

				long fileId = (long)rv;

				rv = HyoutaTools.SqliteUtil.SelectScalar( t, "SELECT id FROM Storage WHERE volumeId = ? AND path = ? AND name = ?", new object[] { volume.ID, dirPath, name } );
				long timestamp = (long)HyoutaTools.Util.DateTimeToUnixTime( lastWriteTimeUtc );
				long lastSeen = (long)HyoutaTools.Util.DateTimeToUnixTime( DateTime.UtcNow );
				if ( rv == null ) {
					HyoutaTools.SqliteUtil.Update( t, "INSERT INTO Storage ( fileId, volumeId, path, name, timestamp, lastSeen ) VALUES ( ?, ?, ?, ?, ?, ? )", new object[] { fileId, volume.ID, dirPath, name, timestamp, lastSeen } );
				} else {
					long storageId = (long)rv;
					HyoutaTools.SqliteUtil.Update( t, "UPDATE Storage SET fileId = ?, timestamp = ?, lastSeen = ? WHERE id = ?", new object[] { fileId, timestamp, lastSeen, storageId } );
				}

				t.Commit();
			}
		}

		private static bool CheckAndUpdateFile( SQLiteConnection connection, Volume volume, FileInfo file, string dirPath, byte[] expectedShorthash ) {
			using ( IDbTransaction t = connection.BeginTransaction() ) {
				var rv = HyoutaTools.SqliteUtil.SelectArray( t, "SELECT Storage.id, Files.size, Storage.timestamp, Files.shorthash FROM Storage INNER JOIN Files ON Storage.fileId = Files.id WHERE Storage.volumeId = ? AND Storage.path = ? AND Storage.name = ?", new object[] { volume.ID, dirPath, file.Name } );
				if ( rv == null || rv.Count == 0 ) {
					return false;
				}

				long storageId = (long)rv[0][0];
				long fileSize = (long)rv[0][1];
				long timestamp = (long)rv[0][2];
				byte[] shorthash = (byte[])rv[0][3];

				long expectedFilesize = file.Length;
				long expectedTimestamp = (long)HyoutaTools.Util.DateTimeToUnixTime( file.LastWriteTimeUtc );

				if ( fileSize != expectedFilesize || timestamp != expectedTimestamp || !expectedShorthash.SequenceEqual( shorthash ) ) {
					return false;
				}

				// seems to check out
				long updateTimestamp = (long)HyoutaTools.Util.DateTimeToUnixTime( DateTime.UtcNow );
				HyoutaTools.SqliteUtil.Update( t, "UPDATE Storage SET lastSeen = ? WHERE id = ?", new object[] { updateTimestamp, storageId } );
				t.Commit();

				return true;
			}
		}

		private static Volume CreateOrFindVolume( SQLiteConnection connection, string id, string label ) {
			using ( IDbTransaction t = connection.BeginTransaction() ) {
				List<object[]> rv = HyoutaTools.SqliteUtil.SelectArray( t, "SELECT id, shouldScan FROM Volumes WHERE guid = ?", new object[] { id } );
				if ( rv == null || rv.Count == 0 ) {
					HyoutaTools.SqliteUtil.Update( t, "INSERT INTO Volumes ( guid, label, shouldScan ) VALUES ( ?, ?, ? )", new object[] { id, label, label.Contains( "ortable" ) } );
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
				//HyoutaTools.SqliteUtil.Update( t, "CREATE INDEX IF NOT EXISTS FileIndex ON Files (size, hash)" );
				HyoutaTools.SqliteUtil.Update( t, "CREATE TABLE IF NOT EXISTS Volumes (" +
					"id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, " +
					"guid TEXT NOT NULL, " +
					"label TEXT NOT NULL, " +
					"shouldScan BOOLEAN NOT NULL" +
				")" );
				HyoutaTools.SqliteUtil.Update( t, "CREATE TABLE IF NOT EXISTS Storage (" +
					"id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, " +
					"fileId INTEGER NOT NULL, " +
					"volumeId INTEGER NOT NULL, " +
					"path TEXT NOT NULL, " + // directory path, unix separator, no drive letter; no filename
					"name TEXT NOT NULL, " + // filename
					"timestamp INTEGER NOT NULL, " + // actual on-disk last modified timestamp
					"lastSeen INTEGER NOT NULL, " + // when we have last seen it
					"FOREIGN KEY(fileId) REFERENCES Files(id), " +
					"FOREIGN KEY(volumeId) REFERENCES Volumes(id), " +
					"UNIQUE(volumeId, path, name)" +
				")" );
				HyoutaTools.SqliteUtil.Update( t, "CREATE INDEX IF NOT EXISTS FilePath ON Storage (path)" );
				t.Commit();
			}
		}
	}
}
