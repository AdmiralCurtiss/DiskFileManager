using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DiskFileManager {
	class DatabaseHelper {
		public static void CreateTables(IDbTransaction t) {
			HyoutaTools.SqliteUtil.Update(t, "CREATE TABLE IF NOT EXISTS Files (" +
				"id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, " +
				"size INTERGER NOT NULL, " +
				"shorthash BLOB NOT NULL, " +
				"hash BLOB NOT NULL, " +
				"UNIQUE(size, shorthash, hash)" +
			")");
			HyoutaTools.SqliteUtil.Update(t, "CREATE TABLE IF NOT EXISTS Volumes (" +
				"id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, " +
				"guid TEXT NOT NULL, " +
				"label TEXT NOT NULL, " +
				"totalSpace INTEGER NOT NULL, " +
				"freeSpace INTEGER NOT NULL, " +
				"shouldScan BOOLEAN NOT NULL" +
			")");
			//HyoutaTools.SqliteUtil.Update(t, "ALTER TABLE Volumes ADD totalSpace INTEGER NOT NULL default 0");
			//HyoutaTools.SqliteUtil.Update(t, "ALTER TABLE Volumes ADD freeSpace INTEGER NOT NULL default 0");
			HyoutaTools.SqliteUtil.Update(t, "CREATE TABLE IF NOT EXISTS Pathnames (" +
				"id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, " +
				"name TEXT NOT NULL, " + // directory path, unix separator, no drive letter; no filename
				"UNIQUE(name)" +
			")");
			HyoutaTools.SqliteUtil.Update(t, "CREATE TABLE IF NOT EXISTS Filenames (" +
				"id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, " +
				"name TEXT NOT NULL, " + // filename
				"UNIQUE(name)" +
			")");
			HyoutaTools.SqliteUtil.Update(t, "CREATE TABLE IF NOT EXISTS Paths (" +
				"id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, " +
				"volumeId INTEGER NOT NULL, " +
				"pathnameId INTEGER NOT NULL, " +
				"FOREIGN KEY(volumeId) REFERENCES Volumes(id), " +
				"FOREIGN KEY(pathnameId) REFERENCES Pathnames(id), " +
				"UNIQUE(volumeId, pathnameId)" +
			")");
			HyoutaTools.SqliteUtil.Update(t, "CREATE TABLE IF NOT EXISTS Storage (" +
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
			")");
			HyoutaTools.SqliteUtil.Update(t, "CREATE INDEX IF NOT EXISTS StorageFileId ON Storage (fileId)");
			HyoutaTools.SqliteUtil.Update(t, "CREATE TABLE IF NOT EXISTS Archives (" +
				"id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL" +
			")");
			HyoutaTools.SqliteUtil.Update(t, "CREATE TABLE IF NOT EXISTS ArchivePaths (" +
				"archiveId INTEGER NOT NULL, " +
				"pathId INTEGER NOT NULL, " +
				"FOREIGN KEY(archiveId) REFERENCES Archives(id), " +
				"FOREIGN KEY(pathId) REFERENCES Paths(id), " +
				"UNIQUE(archiveId, pathId)" +
			")");
			HyoutaTools.SqliteUtil.Update(t, "CREATE TABLE IF NOT EXISTS ArchivePatterns (" +
				"id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, " +
				"archiveId INTEGER NOT NULL, " +
				"pattern TEXT NOT NULL, " +
				"timestampBegin INTEGER NOT NULL, " +
				"timestampEnd INTEGER NOT NULL, " +
				"FOREIGN KEY(archiveId) REFERENCES Archives(id)" +
			")");
		}

		public static long InsertOrUpdateFilename(IDbTransaction t, string name) {
			var rv = HyoutaTools.SqliteUtil.SelectScalar(t, "SELECT id FROM Filenames WHERE name = ?", new object[] { name });
			if (rv == null) {
				HyoutaTools.SqliteUtil.Update(t, "INSERT INTO Filenames ( name ) VALUES ( ? )", new object[] { name });
				rv = HyoutaTools.SqliteUtil.SelectScalar(t, "SELECT id FROM Filenames WHERE name = ?", new object[] { name });
			}
			return (long)rv;
		}

		public static long InsertOrUpdatePathname(IDbTransaction t, string name) {
			var rv = HyoutaTools.SqliteUtil.SelectScalar(t, "SELECT id FROM Pathnames WHERE name = ?", new object[] { name });
			if (rv == null) {
				HyoutaTools.SqliteUtil.Update(t, "INSERT INTO Pathnames ( name ) VALUES ( ? )", new object[] { name });
				rv = HyoutaTools.SqliteUtil.SelectScalar(t, "SELECT id FROM Pathnames WHERE name = ?", new object[] { name });
			}
			return (long)rv;
		}

		public static long InsertOrUpdatePath(IDbTransaction t, long volumeId, string name) {
			long pathnameId = InsertOrUpdatePathname(t, name);
			var rv = HyoutaTools.SqliteUtil.SelectScalar(t, "SELECT id FROM Paths WHERE volumeId = ? AND pathnameId = ?", new object[] { volumeId, pathnameId });
			if (rv == null) {
				HyoutaTools.SqliteUtil.Update(t, "INSERT INTO Paths ( volumeId, pathnameId ) VALUES ( ?, ? )", new object[] { volumeId, pathnameId });
				rv = HyoutaTools.SqliteUtil.SelectScalar(t, "SELECT id FROM Paths WHERE volumeId = ? AND pathnameId = ?", new object[] { volumeId, pathnameId });
			}
			return (long)rv;
		}
	}
}
