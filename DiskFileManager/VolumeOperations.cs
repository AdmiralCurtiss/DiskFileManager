using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DiskFileManager {
	public static class VolumeOperations {
		public static List<Volume> GetKnownVolumes(SQLiteConnection connection) {
			List<Volume> volumes = new List<Volume>();
			using (IDbTransaction t = connection.BeginTransaction()) {
				List<object[]> rv = HyoutaTools.SqliteUtil.SelectArray(t, "SELECT id, guid, label, totalSpace, freeSpace, shouldScan, lastScan, dirty FROM Volumes ORDER BY id ASC", new object[0]);
				if (rv != null) {
					foreach (object[] r in rv) {
						volumes.Add(new Volume() {
							ID = (long)r[0],
							DeviceID = (string)r[1],
							Label = (string)r[2],
							TotalSpace = (long)r[3],
							FreeSpace = (long)r[4],
							ShouldScan = (bool)r[5],
							LastScan = HyoutaTools.Util.UnixTimeToDateTime((long)r[6]),
							Dirty = (long)r[7],
						});
					}
				}

			}
			return volumes;
		}

		public static Volume CreateOrFindVolume(SQLiteConnection connection, string id, string label, long totalSpace, long freeSpace) {
			using (IDbTransaction t = connection.BeginTransaction()) {
				List<object[]> rv = HyoutaTools.SqliteUtil.SelectArray(t, "SELECT id, shouldScan, lastScan, dirty FROM Volumes WHERE guid = ?", new object[] { id });
				long internalId;
				bool shouldScan;
				DateTime lastScan;
				long dirty;
				if (rv == null || rv.Count == 0) {
					HyoutaTools.SqliteUtil.Update(t, "INSERT INTO Volumes ( guid, label, totalSpace, freeSpace, shouldScan ) VALUES ( ?, ?, ?, ?, ? )", new object[] { id, label, totalSpace, freeSpace, true });
					rv = HyoutaTools.SqliteUtil.SelectArray(t, "SELECT id, shouldScan FROM Volumes WHERE guid = ?", new object[] { id });
					internalId = (long)rv[0][0];
					shouldScan = (bool)rv[0][1];
					lastScan = HyoutaTools.Util.UnixTimeToDateTime(0);
					dirty = 1;
				} else {
					internalId = (long)rv[0][0];
					shouldScan = (bool)rv[0][1];
					lastScan = HyoutaTools.Util.UnixTimeToDateTime((long)rv[0][2]);
					dirty = (long)rv[0][3];
					HyoutaTools.SqliteUtil.Update(t, "UPDATE Volumes SET totalSpace = ?, freeSpace = ?, label = ?, dirty = 1 WHERE id = ?", new object[] { totalSpace, freeSpace, label, internalId });
				}
				t.Commit();
				return new Volume() { DeviceID = id, Label = label, ID = internalId, TotalSpace = totalSpace, FreeSpace = freeSpace, ShouldScan = shouldScan, LastScan = lastScan, Dirty = dirty };
			}
		}

		public static List<Volume> FindAndInsertAttachedVolumes(SQLiteConnection connection) {
			List<Volume> volumes = new List<Volume>();
			foreach (Win32Volume vol in Win32Util.GetAttachedVolumes()) {
				volumes.Add(CreateOrFindVolume(connection, vol.Id, vol.Label, (long)vol.Capacity, (long)vol.FreeSpace));
			}
			return volumes;
		}

		public static void SetVolumeEnabledState(SQLiteConnection connection, int volumeId, bool enableVolume) {
			using (IDbTransaction t = connection.BeginTransaction()) {
				HyoutaTools.SqliteUtil.Update(t, "UPDATE Volumes SET shouldScan = ? WHERE id = ?", new object[] { enableVolume ? 1 : 0, volumeId });
				t.Commit();
			}
		}
	}
}
