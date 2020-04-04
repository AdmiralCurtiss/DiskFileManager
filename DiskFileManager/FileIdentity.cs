using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DiskFileManager {
	public partial class FileIdentity {
		public long Filesize { get; private set; }
		public byte[] ShortHash { get { return _ShortHash.Hash; } }
		public byte[] Hash { get { return _Hash.Hash; } }

		private Sha256Hash _ShortHash;
		private Sha256Hash _Hash;

		public FileIdentity(long size, byte[] shorthash, byte[] hash) {
			Filesize = size;
			_ShortHash = new Sha256Hash(shorthash);
			_Hash = new Sha256Hash(hash);
		}

		public FileIdentity(long size, Sha256Hash shorthash, Sha256Hash hash) {
			Filesize = size;
			_ShortHash = shorthash;
			_Hash = hash;
		}

		public override bool Equals(object obj) {
			return obj is FileIdentity identity && this == identity;
		}

		public override int GetHashCode() {
			var hashCode = 1588817190;
			hashCode = hashCode * -1521134295 + Filesize.GetHashCode();
			hashCode = hashCode * -1521134295 + _ShortHash.GetHashCode();
			hashCode = hashCode * -1521134295 + _Hash.GetHashCode();
			return hashCode;
		}

		public static bool operator ==(FileIdentity lhs, FileIdentity rhs) {
			return lhs.Filesize == rhs.Filesize &&
				   lhs._ShortHash == rhs._ShortHash &&
				   lhs._Hash == rhs._Hash;
		}

		public static bool operator !=(FileIdentity lhs, FileIdentity rhs) {
			return !(lhs == rhs);
		}
	}
}
