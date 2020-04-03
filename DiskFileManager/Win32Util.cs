using System.Collections.Generic;
using System.Management;

namespace DiskFileManager {
	public class Win32Volume {
		public string Id;
		public string Label;
		public ulong Capacity;
		public ulong FreeSpace;
	}

	public static class Win32Util {
		public static IEnumerable<Win32Volume> GetAttachedVolumes() {
			foreach (ManagementObject vol in new ManagementClass("Win32_Volume").GetInstances()) {
				string id = vol.Properties["DeviceID"].Value.ToString();
				string label = vol?.Properties["Label"]?.Value?.ToString() ?? "";
				ulong capacity = (ulong)(vol?.Properties["Capacity"]?.Value ?? 0);
				ulong freeSpace = (ulong)(vol?.Properties["FreeSpace"]?.Value ?? 0);
				yield return new Win32Volume() { Id = id, Label = label, Capacity = capacity, FreeSpace = freeSpace };
			}
		}
	}
}
