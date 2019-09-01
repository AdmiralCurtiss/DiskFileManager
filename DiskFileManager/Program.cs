using CommandLine;
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
	public class BaseOptions {
		[Option( "database", Default = null, Required = false, HelpText = "Custom path to database." )]
		public string DatabasePath {
			get {
				return _DatabasePath ?? Path.Combine( Environment.GetFolderPath( Environment.SpecialFolder.MyDocuments ), "diskfilemanagerdb.sqlite" );
			}
			set {
				_DatabasePath = value;
			}
		}

		private string _DatabasePath;

		[Option( "log", Default = null, Required = false, HelpText = "Path to log file. Prints to stdout if not set." )]
		public string LogPath { get; set; }
	}

	[Verb( "scan" )]
	public class ScanOptions : BaseOptions {
	}

	[Verb( "list" )]
	public class ListOptions : BaseOptions {
		[Option( 'v', "volume", Default = null, Required = false, HelpText = "Volume ID to list files of. 0 to list all volumes." )]
		public int? Volume { get; set; }

		[Option( "selected-volume-only", Default = false, Required = false, HelpText = "Only list files on the given volume." )]
		public bool SelectedVolumeOnly { get; set; }

		[Option( "min-instance-count", Default = null, Required = false, HelpText = "Minimum file instance count." )]
		public int? MinInstanceCount { get; set; }

		[Option( "max-instance-count", Default = null, Required = false, HelpText = "Maximum file instance count." )]
		public int? MaxInstanceCount { get; set; }
	}

	[Verb( "search" )]
	public class SearchOptions : BaseOptions {
		[Option( 'f', "filename", Default = null, Required = true, HelpText = "Search pattern for filename." )]
		public string File { get; set; }
	}

	[Verb( "multi" )]
	public class QuickfindMultipleOptions : BaseOptions {
		[Option( 'v', "volume", Required = true, HelpText = "Volume ID to find files of." )]
		public int Volume { get; set; }

		[Option( 'd', "interactive-delete-mode", Default = false, Required = false, HelpText = "Start interactive duplicate deletion mode." )]
		public bool InteractiveDeleteMode { get; set; }

		[Option( "subdir", Default = null, Required = false, HelpText = "Only look in a specific subdirectory." )]
		public string Subdir { get; set; }
	}

	[Verb( "exclusive" )]
	public class QuickfindExclusiveOptions : BaseOptions {
		[Option( 'v', "volume", Required = true, HelpText = "Volume ID to find files of." )]
		public int Volume { get; set; }
	}


	class TextWriterWrapper : IDisposable {
		public TextWriter Writer { get; private set; }

		public TextWriterWrapper( string outpath ) {
			Writer = outpath != null ? new StreamWriter( outpath ) : Console.Out;
		}

		protected virtual void Dispose( bool disposing ) {
			if ( disposing && Writer != Console.Out ) {
				Writer.Dispose();
			}
		}

		public void Dispose() {
			Dispose( true );
		}
	}

	class Program {
		static int Main( string[] args ) {
			return Parser.Default.ParseArguments<ScanOptions, ListOptions, SearchOptions, QuickfindMultipleOptions, QuickfindExclusiveOptions>( args ).MapResult(
				( ScanOptions a ) => Scan( a ),
				( ListOptions a ) => List( a ),
				( SearchOptions a ) => Search( a ),
				( QuickfindMultipleOptions a ) => QuickfindMultipleCopiesOnSameVolume( a ),
				( QuickfindExclusiveOptions a ) => QuickfindFilesExclusiveToVolume( a ),
				errs => -1
			);
		}

		private static int QuickfindMultipleCopiesOnSameVolume( QuickfindMultipleOptions a ) {
			ShouldPrint shouldPrint = ( x ) => x.Count >= 2 && ( a.Subdir == null || x.Any( ( y ) => y.Path.StartsWith( a.Subdir, true, System.Globalization.CultureInfo.InvariantCulture ) ) );
			if ( a.InteractiveDeleteMode ) {
				return RunInteractiveFileDeleteMode( a.DatabasePath, a.Volume, shouldPrint );
			} else {
				return ListFiles( a.LogPath, a.DatabasePath, a.Volume, true, shouldPrint );
			}
		}

		private static int QuickfindFilesExclusiveToVolume( QuickfindExclusiveOptions a ) {
			return ListFiles( a.LogPath, a.DatabasePath, a.Volume, false, ( x ) => x.All( (s) => s.VolumeId == x[0].VolumeId ) );
		}

		private static int Scan( ScanOptions args ) {
			using ( TextWriterWrapper textWriterWrapper = new TextWriterWrapper( args.LogPath ) )
			using ( SQLiteConnection connection = new SQLiteConnection( "Data Source=" + args.DatabasePath ) ) {
				connection.Open();
				using ( IDbTransaction t = connection.BeginTransaction() ) {
					DatabaseHelper.CreateTables( t );
					t.Commit();
				}

				var volumes = FindAndInsertAttachedVolumes( connection );
				foreach ( Volume v in volumes ) {
					ProcessVolume( textWriterWrapper.Writer, connection, v );
				}

				connection.Close();
			}

			return 0;
		}

		private static List<Volume> FindAndInsertAttachedVolumes( SQLiteConnection connection ) {
			List<Volume> volumes = new List<Volume>();
			foreach ( ManagementObject vol in new ManagementClass( "Win32_Volume" ).GetInstances() ) {
				string id = vol.Properties["DeviceID"].Value.ToString();
				string label = vol?.Properties["Label"]?.Value?.ToString() ?? "";
				ulong capacity = (ulong)( vol?.Properties["Capacity"]?.Value ?? 0 );
				ulong freeSpace = (ulong)( vol?.Properties["FreeSpace"]?.Value ?? 0 );
				volumes.Add( CreateOrFindVolume( connection, id, label, (long)capacity, (long)freeSpace ) );
			}
			return volumes;
		}

		private static int List( ListOptions args ) {
			if ( args.Volume.HasValue ) {
				int? volume = null;
				if ( args.Volume.Value != 0 ) {
					volume = args.Volume.Value;
				}
				return ListFiles( args.LogPath, args.DatabasePath, volume, args.SelectedVolumeOnly, ( x ) => {
					bool minLimit = args.MinInstanceCount == null || x.Count >= args.MinInstanceCount.Value;
					bool maxLimit = args.MaxInstanceCount == null || x.Count <= args.MaxInstanceCount.Value;
					return minLimit && maxLimit;
				} );
			} else {
				return ListVolumes( args.LogPath, args.DatabasePath );
			}
		}

		private static int ListVolumes( string logPath, string databasePath ) {
			using ( TextWriterWrapper textWriterWrapper = new TextWriterWrapper( logPath ) )
			using ( SQLiteConnection connection = new SQLiteConnection( "Data Source=" + databasePath ) ) {
				connection.Open();
				PrintVolumeInformation( textWriterWrapper.Writer, GetKnownVolumes( connection ) );
				connection.Close();
			}

			return 0;
		}

		private static int RunInteractiveFileDeleteMode( string databasePath, long volumeId, ShouldPrint shouldPrint ) {
			using ( SQLiteConnection connection = new SQLiteConnection( "Data Source=" + databasePath ) ) {
				connection.Open();

				Volume volume;
				{
					var volumes = FindAndInsertAttachedVolumes( connection );
					volume = volumes.FirstOrDefault( ( x ) => x.ID == volumeId );
				}
				if ( volume == null ) {
					Console.WriteLine( "Volume {0} is not attached.", volumeId );
					return -1;
				}


				List<StorageFile> files = GetKnownFilesOnVolume( connection, volumeId );
				foreach ( var sameFiles in CollectFiles( connection, files, shouldPrint, volumeId ) ) {
					Console.WriteLine();
					Console.WriteLine( " ================================================================== " );
					Console.WriteLine();
					Console.WriteLine( "File #{0}", sameFiles[0].FileId );
					Console.WriteLine( "{0:N0} bytes", sameFiles[0].Size );
					Console.WriteLine( "Exists in {0} places:", sameFiles.Count );
					for ( int i = 0; i < sameFiles.Count; ++i ) {
						var sf = sameFiles[i];
						Console.WriteLine( " No. {0}: {1}/{2}", i, sf.Path, sf.Filename );
					}

					while ( true ) {
						Console.WriteLine( "Enter number of file to keep, nothing to skip, q to quit." );
						Console.Write( " > " );

						string input = Console.ReadLine();
						if ( input == "" ) {
							break;
						}
						if ( input == "q" ) {
							return -2;
						}

						int number;
						if ( int.TryParse( input, out number ) ) {
							if ( number >= 0 && number < sameFiles.Count ) {
								List<(FileInfo fi, bool shouldDelete)> data = new List<(FileInfo fi, bool shouldDelete)>();
								for ( int i = 0; i < sameFiles.Count; ++i ) {
									var sf = sameFiles[i];
									string path;
									if ( sf.Path == "" || sf.Path == "/" || sf.Path == "\\" ) {
										path = Path.Combine( volume.DeviceID, sf.Filename );
									} else if ( sf.Path[0] == '/' || sf.Path[0] == '\\' ) {
										path = Path.Combine( volume.DeviceID, sf.Path.Substring( 1 ).Replace( '/', '\\' ), sf.Filename );
									} else {
										Console.WriteLine( "Unexpected path in database: " + sf.Path );
										break;
									}
									data.Add( (new FileInfo( path ), i != number) );
								}

								bool inconsistent = false;
								foreach ( var d in data ) {
									if ( !d.fi.Exists ) {
										Console.WriteLine( "Inconsistent file state: File {0} does not actually exist on disk.", d.fi.FullName );
										inconsistent = true;
									}
								}
								if ( !inconsistent ) {
									foreach ( var d in data ) {
										if ( d.shouldDelete ) {
											Console.WriteLine( "Deleting {0}", d.fi.FullName );
											d.fi.Delete();
										} else {
											Console.WriteLine( "Keeping {0}", d.fi.FullName );
										}
									}
								}
								break;
							}
						}
					}
				}

				connection.Close();
			}

			return 0;
		}

		private static int ListFiles( string logPath, string databasePath, int? volume, bool selectedVolumeOnly, ShouldPrint shouldPrint ) {
			using ( TextWriterWrapper textWriterWrapper = new TextWriterWrapper( logPath ) )
			using ( SQLiteConnection connection = new SQLiteConnection( "Data Source=" + databasePath ) ) {
				connection.Open();

				List<StorageFile> files = GetKnownFilesOnVolume( connection, volume );
				foreach ( var sameFiles in CollectFiles( connection, files, shouldPrint, selectedVolumeOnly ? volume : null ) ) {
					PrintSameFileInformation( textWriterWrapper.Writer, sameFiles );
				}

				connection.Close();
			}

			return 0;
		}

		private static int Search( SearchOptions args ) {
			using ( TextWriterWrapper textWriterWrapper = new TextWriterWrapper( args.LogPath ) )
			using ( SQLiteConnection connection = new SQLiteConnection( "Data Source=" + args.DatabasePath ) ) {
				connection.Open();
				foreach ( var sameFiles in CollectFiles( connection, GetFilesWithFilename( connection, "%" + args.File + "%" ), (x) => true ) ) {
					PrintSameFileInformation( textWriterWrapper.Writer, sameFiles );
				}
				connection.Close();
			}

			return 0;
		}

		private static void PrintVolumeInformation( TextWriter stdout, List<Volume> volumes ) {
			foreach ( var volume in volumes ) {
				stdout.WriteLine( "Volume #{0}: {1}", volume.ID, volume.Label );
			}
		}

		private static void PrintSameFileInformation( TextWriter stdout, List<StorageFile> sameFiles ) {
			if ( sameFiles.Count > 0 ) {
				stdout.WriteLine( "File #{0}", sameFiles[0].FileId );
				stdout.WriteLine( "{0:N0} bytes", sameFiles[0].Size );
				stdout.WriteLine( "Exists in {0} places:", sameFiles.Count );
				foreach ( var sf in sameFiles ) {
					stdout.WriteLine( "  Volume #{0}, {1}/{2}", sf.VolumeId, sf.Path, sf.Filename );
				}
				stdout.WriteLine();
			}
		}

		private delegate bool ShouldPrint( List<StorageFile> files );
		private static IEnumerable<List<StorageFile>> CollectFiles( SQLiteConnection connection, List<StorageFile> files, ShouldPrint shouldPrint, long? onlyOnVolume = null ) {
			ISet<long> seenIds = new HashSet<long>();
			foreach ( var file in files ) {
				if ( seenIds.Contains( file.FileId ) ) {
					continue;
				}

				seenIds.Add( file.FileId );
				var sameFiles = GetStorageFilesForFileId( connection, file.FileId, onlyOnVolume );
				if ( shouldPrint( sameFiles ) ) {
					yield return sameFiles;
				}
			}
		}

		private static List<Volume> GetKnownVolumes( SQLiteConnection connection ) {
			List<Volume> volumes = new List<Volume>();
			using ( IDbTransaction t = connection.BeginTransaction() ) {
				List<object[]> rv = HyoutaTools.SqliteUtil.SelectArray( t, "SELECT id, guid, label, totalSpace, freeSpace, shouldScan FROM Volumes ORDER BY id ASC", new object[0] );
				if ( rv != null ) {
					foreach ( object[] r in rv ) {
						volumes.Add( new Volume() {
							ID = (long)r[0],
							DeviceID = (string)r[1],
							Label = (string)r[2],
							TotalSpace = (long)r[3],
							FreeSpace = (long)r[4],
							ShouldScan = (bool)r[5],
						} );
					}
				}

			}
			return volumes;
		}

		private static List<StorageFile> GetStorageFilesForFileId( SQLiteConnection connection, long fileId, long? onlyOnVolume = null ) {
			var rv = HyoutaTools.SqliteUtil.SelectArray( connection,
				"SELECT Files.size, Files.shorthash, Files.hash, Storage.id AS storageId, Pathnames.name AS pathname, " +
				"Paths.volumeId, Filenames.name AS filename, Storage.timestamp, Storage.lastSeen " +
				"FROM Files " +
				"INNER JOIN Storage ON Files.id = Storage.fileId " +
				"INNER JOIN Paths ON Storage.pathId = Paths.id " +
				"INNER JOIN Pathnames ON Paths.pathnameId = Pathnames.id " +
				"INNER JOIN Filenames ON Storage.filenameId = Filenames.id " +
				"WHERE Files.id = ?" + ( onlyOnVolume != null ? " AND Paths.volumeId = ?" : "" ),
				onlyOnVolume != null ? new object[] { fileId, onlyOnVolume.Value } : new object[] { fileId }
			);

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
					Timestamp = HyoutaTools.Util.UnixTimeToDateTime( (long)arr[7] ),
					LastSeen = HyoutaTools.Util.UnixTimeToDateTime( (long)arr[8] ),
				} );
			}

			return files;
		}

		private static List<StorageFile> GetKnownFilesOnVolume( SQLiteConnection connection, long? volumeId ) {
			string statement = "SELECT Files.id, Files.size, Files.shorthash, Files.hash, Storage.id AS storageId, " +
				"Pathnames.name AS pathname, Filenames.name AS filename, Storage.timestamp, Storage.lastSeen, Paths.volumeId AS volumeId " +
				"FROM Storage " +
				"INNER JOIN Files ON Storage.fileId = Files.id " +
				"INNER JOIN Paths ON Storage.pathId = Paths.id " +
				"INNER JOIN Pathnames ON Paths.pathnameId = Pathnames.id " +
				"INNER JOIN Filenames ON Storage.filenameId = Filenames.id";

			List<object[]> rv;
			if ( volumeId.HasValue ) {
				rv = HyoutaTools.SqliteUtil.SelectArray( connection, statement + " WHERE Paths.volumeId = ?", new object[] { volumeId.Value } );
			} else {
				rv = HyoutaTools.SqliteUtil.SelectArray( connection, statement, new object[0] );
			}

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
					VolumeId = (long)arr[9],
					Timestamp = HyoutaTools.Util.UnixTimeToDateTime( (long)arr[7] ),
					LastSeen = HyoutaTools.Util.UnixTimeToDateTime( (long)arr[8] ),
				} );
			}

			return files;
		}

		private static List<StorageFile> GetFilesWithFilename( SQLiteConnection connection, string filename ) {
			var rv = HyoutaTools.SqliteUtil.SelectArray( connection,
				"SELECT Files.id, Files.size, Files.shorthash, Files.hash, Storage.id AS storageId, Paths.volumeId, " +
				"Pathnames.name AS pathname, Filenames.name AS filename, Storage.timestamp, Storage.lastSeen " +
				"FROM Storage " +
				"INNER JOIN Files ON Storage.fileId = Files.id " +
				"INNER JOIN Paths ON Storage.pathId = Paths.id " +
				"INNER JOIN Pathnames ON Paths.pathnameId = Pathnames.id " +
				"INNER JOIN Filenames ON Storage.filenameId = Filenames.id " +
				"WHERE Filenames.name LIKE ?", new object[] { filename } );

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
					VolumeId = (long)arr[5],
					Path = (string)arr[6],
					Filename = (string)arr[7],
					Timestamp = HyoutaTools.Util.UnixTimeToDateTime( (long)arr[8] ),
					LastSeen = HyoutaTools.Util.UnixTimeToDateTime( (long)arr[9] ),
				} );
			}

			return files;
		}

		private static List<StorageFile> ProcessVolume( TextWriter stdout, SQLiteConnection connection, Volume volume ) {
			if ( !volume.ShouldScan ) {
				return null;
			}

			List<StorageFile> files = new List<StorageFile>();
			ProcessDirectory( stdout, connection, files, volume, new DirectoryInfo( volume.DeviceID ), "" );
			DiscardUnseenStorageFiles( stdout, connection, files, volume );
			return files;
		}

		private static void DiscardUnseenStorageFiles( TextWriter stdout, SQLiteConnection connection, List<StorageFile> files, Volume volume ) {
			var knownFiles = GetKnownFilesOnVolume( connection, volume.ID );
			ISet<long> seenStorageIds = new HashSet<long>();
			foreach ( var f in files ) {
				seenStorageIds.Add( f.StorageId );
			}
			using ( IDbTransaction t = connection.BeginTransaction() ) {
				foreach ( var f in knownFiles ) {
					if ( !seenStorageIds.Contains( f.StorageId ) ) {
						stdout.WriteLine( "[" + volume.Label + "] Discarding unseen file {0}/{1}", f.Path, f.Filename );
						HyoutaTools.SqliteUtil.Update( t, "DELETE FROM Storage WHERE id = ?", new object[] { f.StorageId } );
					}
				}
				t.Commit();
			}
		}

		private static void ProcessDirectory( TextWriter stdout, SQLiteConnection connection, List<StorageFile> files, Volume volume, DirectoryInfo directory, string path ) {
			try {
				foreach ( var fsi in directory.GetFileSystemInfos() ) {
					if ( fsi is FileInfo ) {
						ProcessFile( stdout, connection, files, volume, fsi as FileInfo, path );
					} else if ( fsi is DirectoryInfo ) {
						ProcessDirectory( stdout, connection, files, volume, fsi as DirectoryInfo, path + "/" + fsi.Name );
					}
				}
			} catch ( UnauthorizedAccessException ex ) {
				stdout.WriteLine( ex.ToString() );
			}
		}

		private static void ProcessFile( TextWriter stdout, SQLiteConnection connection, List<StorageFile> files, Volume volume, FileInfo file, string dirPath ) {
			try {
				stdout.Write( "[" + volume.Label + "] Checking file: " + dirPath + "/" + file.Name + ", " + string.Format( "{0:n0}", file.Length ) + " bytes..." );
				byte[] shorthash;
				using ( var fs = new FileStream( file.FullName, FileMode.Open, FileAccess.Read ) ) {
					shorthash = HashUtil.CalculateShortHash( fs );
				}
				StorageFile sf = CheckAndUpdateFile( connection, volume, file, dirPath, shorthash );
				if ( sf != null ) {
					stdout.WriteLine( " seems same." );
					files.Add( sf );
					return;
				}
				stdout.WriteLine( " is different or new." );

				long filesize;
				byte[] hash;
				using ( var fs = new FileStream( file.FullName, FileMode.Open, FileAccess.Read ) ) {
					filesize = fs.Length;
					shorthash = HashUtil.CalculateShortHash( fs );
					hash = HashUtil.CalculateHash( fs );
				}
				files.Add( InsertOrUpdateFile( connection, volume, dirPath, file.Name, filesize, hash, shorthash, file.LastWriteTimeUtc ) );
			} catch ( UnauthorizedAccessException ex ) {
				stdout.WriteLine( ex.ToString() );
			}
		}

		private static StorageFile InsertOrUpdateFile( SQLiteConnection connection, Volume volume, string dirPath, string name, long filesize, byte[] hash, byte[] shorthash, DateTime lastWriteTimeUtc ) {
			using ( IDbTransaction t = connection.BeginTransaction() ) {
				var rv = HyoutaTools.SqliteUtil.SelectScalar( t, "SELECT id FROM Files WHERE size = ? AND hash = ? AND shorthash = ?", new object[] { filesize, hash, shorthash } );
				if ( rv == null ) {
					HyoutaTools.SqliteUtil.Update( t, "INSERT INTO Files ( size, hash, shorthash ) VALUES ( ?, ?, ? )", new object[] { filesize, hash, shorthash } );
					rv = HyoutaTools.SqliteUtil.SelectScalar( t, "SELECT id FROM Files WHERE size = ? AND hash = ? AND shorthash = ?", new object[] { filesize, hash, shorthash } );
				}

				long fileId = (long)rv;
				long pathId = DatabaseHelper.InsertOrUpdatePath( t, volume.ID, dirPath );
				long filenameId = DatabaseHelper.InsertOrUpdateFilename( t, name );

				rv = HyoutaTools.SqliteUtil.SelectScalar( t, "SELECT id FROM Storage WHERE pathId = ? AND filenameId = ?", new object[] { pathId, filenameId } );
				long timestamp = HyoutaTools.Util.DateTimeToUnixTime( lastWriteTimeUtc );
				long lastSeen = HyoutaTools.Util.DateTimeToUnixTime( DateTime.UtcNow );
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
					Timestamp = HyoutaTools.Util.UnixTimeToDateTime( timestamp ),
					LastSeen = HyoutaTools.Util.UnixTimeToDateTime( lastSeen ),
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
				long expectedTimestamp = HyoutaTools.Util.DateTimeToUnixTime( file.LastWriteTimeUtc );

				if ( fileSize != expectedFilesize || timestamp != expectedTimestamp || !expectedShorthash.SequenceEqual( shorthash ) ) {
					return null;
				}

				// seems to check out
				long updateTimestamp = HyoutaTools.Util.DateTimeToUnixTime( DateTime.UtcNow );
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
					Timestamp = HyoutaTools.Util.UnixTimeToDateTime( timestamp ),
					LastSeen = HyoutaTools.Util.UnixTimeToDateTime( updateTimestamp ),
				};
			}
		}

		private static Volume CreateOrFindVolume( SQLiteConnection connection, string id, string label, long totalSpace, long freeSpace ) {
			using ( IDbTransaction t = connection.BeginTransaction() ) {
				List<object[]> rv = HyoutaTools.SqliteUtil.SelectArray( t, "SELECT id, shouldScan FROM Volumes WHERE guid = ?", new object[] { id } );
				long internalId;
				if ( rv == null || rv.Count == 0 ) {
					HyoutaTools.SqliteUtil.Update( t, "INSERT INTO Volumes ( guid, label, totalSpace, freeSpace, shouldScan ) VALUES ( ?, ?, ?, ?, ? )", new object[] { id, label, totalSpace, freeSpace, true } );
					rv = HyoutaTools.SqliteUtil.SelectArray( t, "SELECT id, shouldScan FROM Volumes WHERE guid = ?", new object[] { id } );
					internalId = (long)rv[0][0];
				} else {
					internalId = (long)rv[0][0];
					HyoutaTools.SqliteUtil.Update( t, "UPDATE Volumes SET totalSpace = ?, freeSpace = ? WHERE id = ?", new object[] { totalSpace, freeSpace, internalId } );
				}
				t.Commit();
				return new Volume() { DeviceID = id, Label = label, ID = internalId, TotalSpace = totalSpace, FreeSpace = freeSpace, ShouldScan = (bool)rv[0][1] };
			}
		}
	}
}
