using System;

namespace DiskFileManager {
	public class Sha256Hash {
		private static readonly int LENGTH = 32;
		public byte[] Hash;

		public Sha256Hash(byte[] hash) {
			if (hash.Length != LENGTH) {
				throw new Exception("Invalid SHA-256 length.");
			}
			Hash = hash;
		}

		public override bool Equals(object obj) {
			return obj is Sha256Hash hash && this == hash;
		}

		public override int GetHashCode() {
			int hash = -1545866855;
			for (int i = 0; i < LENGTH; i += 4) {
				byte b0 = Hash[i + 0];
				byte b1 = Hash[i + 1];
				byte b2 = Hash[i + 2];
				byte b3 = Hash[i + 3];
				uint h = ((uint)b0) + (((uint)b1) << 8) + (((uint)b2) << 16) + (((uint)b3) << 24);
				hash += (int)h;
			}
			return hash;
		}

		public static bool operator ==(Sha256Hash lhs, Sha256Hash rhs) {
			for (int i = 0; i < LENGTH; ++i) {
				if (lhs.Hash[i] != rhs.Hash[i]) {
					return false;
				}
			}
			return true;
		}

		public static bool operator !=(Sha256Hash lhs, Sha256Hash rhs) {
			return !(lhs == rhs);
		}
	}
}
