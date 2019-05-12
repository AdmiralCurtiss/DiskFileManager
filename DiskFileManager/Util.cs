using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Text.RegularExpressions;

namespace HyoutaTools {
	public static class Util {
		#region TimeUtils
		public static DateTime UnixTimeToDateTime( long unixTime ) {
			return new DateTime( 1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc ).AddSeconds( unixTime ).ToLocalTime();
		}
		public static long DateTimeToUnixTime( DateTime time ) {
			return (long)( time - new DateTime( 1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc ).ToLocalTime() ).TotalSeconds;
		}
		#endregion
	}
}
