using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace DiskFileManager {
	public static class HashUtil {
		public static byte[] CalculateHash( Stream stream ) {
			using ( var sha256 = SHA256.Create() ) {
				long filesize = stream.Length;
				stream.Position = 0;
				byte[] hash = sha256.ComputeHash( stream );
				return hash;
			}
		}

		public static byte[] CalculateShortHash( Stream stream ) {
			int bytesBegin = 16 * 1024;
			int bytesMiddle = 32 * 1024;
			int bytesEnd = 16 * 1024;
			int bytesTotal = bytesBegin + bytesMiddle + bytesEnd;

			if ( stream.Length <= bytesTotal ) {
				return CalculateHash( stream );
			}

			byte[] data = new byte[bytesTotal];
			stream.Position = 0;
			stream.Read( data, 0, bytesBegin );

			stream.Position = data.Length / 2 - bytesMiddle / 2;
			stream.Read( data, bytesBegin, bytesMiddle );

			stream.Position = stream.Length - bytesEnd;
			stream.Read( data, bytesBegin + bytesMiddle, bytesEnd );

			using ( var sha256 = SHA256.Create() ) {
				byte[] hash = sha256.ComputeHash( data );
				return hash;
			}
		}
	}
}
